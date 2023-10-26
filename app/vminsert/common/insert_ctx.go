package common

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// InsertCtx contains common bits for data points insertion.
type InsertCtx struct {
	Labels sortedLabels

	mrs            []storage.MetricRow
	metricNamesBuf []byte

	relabelCtx    relabel.Ctx
	streamAggrCtx streamAggrCtx

	skipStreamAggr bool
}

// Reset resets ctx for future fill with rowsLen rows.
// 重置ctx以便将来填充rowsLen行。
func (ctx *InsertCtx) Reset(rowsLen int) {
	//遍历所有的标签，并且进行重置
	for i := range ctx.Labels {
		//这里获取标签的指针地址，这样就赋值修改就能对原有值生效，否则的话只是赋值，而不是指针地址
		label := &ctx.Labels[i]
		label.Name = nil
		label.Value = nil
	}
	//重置标签的长度
	ctx.Labels = ctx.Labels[:0]

	mrs := ctx.mrs
	//重置metricRow
	for i := range mrs {
		cleanMetricRow(&mrs[i])
	}
	//重置metric的长度
	ctx.mrs = mrs[:0]

	//如果rowsLen大于cap(ctx.mrs)的话，就进行扩容
	if n := rowsLen - cap(ctx.mrs); n > 0 {
		ctx.mrs = append(ctx.mrs[:cap(ctx.mrs)], make([]storage.MetricRow, n)...)
	}
	//长度置位0
	ctx.mrs = ctx.mrs[:0]
	//metric name也置位0
	ctx.metricNamesBuf = ctx.metricNamesBuf[:0]
	ctx.relabelCtx.Reset()
	ctx.streamAggrCtx.Reset()
	ctx.skipStreamAggr = false
}

func cleanMetricRow(mr *storage.MetricRow) {
	mr.MetricNameRaw = nil
}

func (ctx *InsertCtx) marshalMetricNameRaw(prefix []byte, labels []prompb.Label) []byte {
	start := len(ctx.metricNamesBuf)
	ctx.metricNamesBuf = append(ctx.metricNamesBuf, prefix...)
	ctx.metricNamesBuf = storage.MarshalMetricNameRaw(ctx.metricNamesBuf, labels)
	metricNameRaw := ctx.metricNamesBuf[start:]
	return metricNameRaw[:len(metricNameRaw):len(metricNameRaw)]
}

// WriteDataPoint writes (timestamp, value) with the given prefix and labels into ctx buffer.
func (ctx *InsertCtx) WriteDataPoint(prefix []byte, labels []prompb.Label, timestamp int64, value float64) error {
	//生成新的标签名称,主要是生成唯一的索引主键，根据metric还有labels
	metricNameRaw := ctx.marshalMetricNameRaw(prefix, labels)
	//添加行内容
	return ctx.addRow(metricNameRaw, timestamp, value)
}

// WriteDataPointExt writes (timestamp, value) with the given metricNameRaw and labels into ctx buffer.
//
// It returns metricNameRaw for the given labels if len(metricNameRaw) == 0.
func (ctx *InsertCtx) WriteDataPointExt(metricNameRaw []byte, labels []prompb.Label, timestamp int64, value float64) ([]byte, error) {
	if len(metricNameRaw) == 0 {
		metricNameRaw = ctx.marshalMetricNameRaw(nil, labels)
	}
	err := ctx.addRow(metricNameRaw, timestamp, value)
	return metricNameRaw, err
}

func (ctx *InsertCtx) addRow(metricNameRaw []byte, timestamp int64, value float64) error {
	mrs := ctx.mrs
	//这个函数的目的是在 mrs 切片的末尾添加一个新的 storage.MetricRow。这个新的 storage.MetricRow 用于存储传入的 metricNameRaw、timestamp 和 value。
	if cap(mrs) > len(mrs) {
		mrs = mrs[:len(mrs)+1]
	} else {
		mrs = append(mrs, storage.MetricRow{})
	}
	//获取最后一个元素
	mr := &mrs[len(mrs)-1]
	ctx.mrs = mrs
	//对最后一个元素赋值
	mr.MetricNameRaw = metricNameRaw
	mr.Timestamp = timestamp
	mr.Value = value
	//确保缓存区不会变的过大，是否超出16MB，如果超出则刷新写到磁盘
	if len(ctx.metricNamesBuf) > 16*1024*1024 {
		if err := ctx.FlushBufs(); err != nil {
			return err
		}
	}
	return nil
}

// AddLabelBytes adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtx) AddLabelBytes(name, value []byte) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.Labels = append(ctx.Labels, prompb.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  name,
		Value: value,
	})
}

// AddLabel adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtx) AddLabel(name, value string) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.Labels = append(ctx.Labels, prompb.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  bytesutil.ToUnsafeBytes(name),
		Value: bytesutil.ToUnsafeBytes(value),
	})
}

// ApplyRelabeling applies relabeling to ic.Labels.
func (ctx *InsertCtx) ApplyRelabeling() {
	ctx.Labels = ctx.relabelCtx.ApplyRelabeling(ctx.Labels)
}

// FlushBufs flushes buffered rows to the underlying storage.
func (ctx *InsertCtx) FlushBufs() error {
	sas := sasGlobal.Load()
	if sas != nil && !ctx.skipStreamAggr {
		matchIdxs := matchIdxsPool.Get()
		matchIdxs.B = ctx.streamAggrCtx.push(ctx.mrs, matchIdxs.B)
		if !*streamAggrKeepInput {
			// Remove aggregated rows from ctx.mrs
			ctx.dropAggregatedRows(matchIdxs.B)
		}
		matchIdxsPool.Put(matchIdxs)
	}
	// There is no need in limiting the number of concurrent calls to vmstorage.AddRows() here,
	// since the number of concurrent FlushBufs() calls should be already limited via writeconcurrencylimiter
	// used at every stream.Parse() call under lib/protoparser/*
	err := vmstorage.AddRows(ctx.mrs)
	ctx.Reset(0)
	if err == nil {
		return nil
	}
	return &httpserver.ErrorWithStatusCode{
		Err:        fmt.Errorf("cannot store metrics: %w", err),
		StatusCode: http.StatusServiceUnavailable,
	}
}

func (ctx *InsertCtx) dropAggregatedRows(matchIdxs []byte) {
	dst := ctx.mrs[:0]
	src := ctx.mrs
	if !*streamAggrDropInput {
		for idx, match := range matchIdxs {
			if match == 1 {
				continue
			}
			dst = append(dst, src[idx])
		}
	}
	tail := src[len(dst):]
	for i := range tail {
		cleanMetricRow(&tail[i])
	}
	ctx.mrs = dst
}

var matchIdxsPool bytesutil.ByteBufferPool
