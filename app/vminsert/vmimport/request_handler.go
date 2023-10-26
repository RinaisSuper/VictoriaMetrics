package vmimport

import (
	"net/http"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/vmimport"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/vmimport/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted  = metrics.NewCounter(`vm_rows_inserted_total{type="vmimport"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="vmimport"}`)
)

// InsertHandler processes `/api/v1/import` request.
//
// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6
func InsertHandler(req *http.Request) error {
	//提取请求中的额外标签，后续这些标签也要插入的
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	//判断是不是gzip格式的压缩数据
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	//解析请求中的数据，然后进行回调处理来写入数据
	return stream.Parse(req.Body, isGzipped, func(rows []parser.Row) error {
		return insertRows(rows, extraLabels)
	})
}

func insertRows(rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := getPushCtx()
	defer putPushCtx(ctx)

	rowsLen := 0
	for i := range rows {
		rowsLen += len(rows[i].Values)
	}
	ic := &ctx.Common
	ic.Reset(rowsLen)
	rowsTotal := 0
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		rowsTotal += len(r.Values)
		ic.Labels = ic.Labels[:0]
		for j := range r.Tags {
			tag := &r.Tags[j]
			ic.AddLabelBytes(tag.Key, tag.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ic.AddLabel(label.Name, label.Value)
		}
		if hasRelabeling {
			ic.ApplyRelabeling()
		}
		if len(ic.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		ic.SortLabelsIfNeeded()
		ctx.metricNameBuf = storage.MarshalMetricNameRaw(ctx.metricNameBuf[:0], ic.Labels)
		values := r.Values
		timestamps := r.Timestamps
		if len(timestamps) != len(values) {
			logger.Panicf("BUG: len(timestamps)=%d must match len(values)=%d", len(timestamps), len(values))
		}
		for j, value := range values {
			timestamp := timestamps[j]
			if err := ic.WriteDataPoint(ctx.metricNameBuf, nil, timestamp, value); err != nil {
				return err
			}
		}
	}
	rowsInserted.Add(rowsTotal)
	rowsPerInsert.Update(float64(rowsTotal))
	return ic.FlushBufs()
}

type pushCtx struct {
	Common        common.InsertCtx
	metricNameBuf []byte
}

func (ctx *pushCtx) reset() {
	ctx.Common.Reset(0)
	ctx.metricNameBuf = ctx.metricNameBuf[:0]
}

/*
*
<pre>
让我们详细解释这个函数的行为：

使用 select 语句尝试从 pushCtxPoolCh channel 中读取一个 pushCtx 对象。
如果 channel 中有一个可用的 pushCtx，则立即返回它。
如果 channel 是空的（即没有可用的 pushCtx），则执行 select 语句的 default 代码块。
在 default 代码块中：
尝试从 pushCtxPool 对象池中获取一个 pushCtx 对象。
如果对象池中有一个可用的 pushCtx，则返回它。
如果对象池为空，则创建一个新的 pushCtx 对象并返回。
这个函数的设计意味着它首先会尝试从 pushCtxPoolCh channel 中获取一个 pushCtx，如果 channel 是空的，它会退回到 pushCtxPool 对象池。如果对象池也是空的，它会创建一个全新的 pushCtx 对象。这种设计提供了两级缓存机制，优先使用 channel，然后使用对象池，最后是新对象的创建。

这种方法是为了在高并发环境下提供更好的性能，因为从 channel 读取通常比从对象池获取更快。但如果 channel 是空的，对象池提供了一个备选方案，避免了不必要的对象创建。
</pre>
*/
func getPushCtx() *pushCtx {
	select {
	case ctx := <-pushCtxPoolCh:
		return ctx
	default:
		if v := pushCtxPool.Get(); v != nil {
			return v.(*pushCtx)
		}
		return &pushCtx{}
	}
}

func putPushCtx(ctx *pushCtx) {
	ctx.reset()
	select {
	case pushCtxPoolCh <- ctx:
	default:
		pushCtxPool.Put(ctx)
	}
}

var pushCtxPool sync.Pool
var pushCtxPoolCh = make(chan *pushCtx, cgroup.AvailableCPUs())
