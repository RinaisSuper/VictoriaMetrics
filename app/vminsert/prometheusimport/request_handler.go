package prometheusimport

import (
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus/stream"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted  = metrics.NewCounter(`vm_rows_inserted_total{type="prometheus"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="prometheus"}`)
)

// InsertHandler processes `/api/v1/import/prometheus` request.
func InsertHandler(req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	defaultTimestamp, err := parserCommon.GetTimestamp(req)
	if err != nil {
		return err
	}
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	return stream.Parse(req.Body, defaultTimestamp, isGzipped, func(rows []parser.Row) error {
		return insertRows(rows, extraLabels)
	}, func(s string) {
		httpserver.LogError(req, s)
	})
}

func insertRows(rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := common.GetInsertCtx()
	defer common.PutInsertCtx(ctx)

	//重置ctc
	ctx.Reset(len(rows))
	//判断是否需要重新label
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		//增加标签，默认标签"",metric的标签指标名
		ctx.AddLabel("", r.Metric)
		//添加标签
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		//添加额外标签
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		if hasRelabeling {
			//标签重定义
			ctx.ApplyRelabeling()
		}
		//如果没有标签，则不处理，说明是无效的
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		//对标签进行排序
		ctx.SortLabelsIfNeeded()
		//进行数据写入
		if err := ctx.WriteDataPoint(nil, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}

	}
	rowsInserted.Add(len(rows))
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
