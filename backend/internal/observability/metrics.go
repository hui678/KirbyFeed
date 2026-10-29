package observability

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// 这里刻意不引入 Prometheus 客户端：项目只需要少量 counter/gauge，用标准库
// 导出 Prometheus 文本格式就够了，少一个依赖、也少一套版本升级负担。
//
// 指标按「名字 + 标签集合」动态注册。名字用 Prometheus 约定后缀：
// counter 以 _total 结尾，gauge 不带后缀。所有名字统一加 feedsystem_ 前缀，
// 避免与其他 exporter 撞名。

type metric struct {
	name   string
	labels string
	gauge  bool
	value  atomic.Int64
}

var registry sync.Map // "name\x00labels" -> *metric

func metricKey(name, labels string) string { return name + "\x00" + labels }

func getMetric(name, labels string, gauge bool) *metric {
	key := metricKey(name, labels)
	if v, ok := registry.Load(key); ok {
		return v.(*metric)
	}
	m := &metric{name: name, labels: labels, gauge: gauge}
	actual, _ := registry.LoadOrStore(key, m)
	return actual.(*metric)
}

// Inc 给 counter 加 1。kv 是成对的标签名和值，例如 "queue", "like.events"。
func Inc(name string, kv ...string) { Add(name, 1, kv...) }

// Add 给 counter 加任意增量。
func Add(name string, delta int64, kv ...string) {
	getMetric(name, renderLabels(kv), false).value.Add(delta)
}

// SetGauge 设置 gauge 的当前值（不是累加）。
func SetGauge(name string, v int64, kv ...string) {
	getMetric(name, renderLabels(kv), true).value.Store(v)
}

func renderLabels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i])
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(kv[i+1]))
		b.WriteString(`"`)
	}
	return b.String()
}

func escapeLabelValue(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Handler 以 Prometheus 文本格式输出当前进程的全部指标。
// 注意：每个进程各有一份独立的 registry，API 和 Worker 需要分别抓取。
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		items := make([]*metric, 0, 32)
		registry.Range(func(_, v any) bool {
			items = append(items, v.(*metric))
			return true
		})
		sort.Slice(items, func(i, j int) bool {
			if items[i].name != items[j].name {
				return items[i].name < items[j].name
			}
			return items[i].labels < items[j].labels
		})

		var b strings.Builder
		lastName := ""
		for _, m := range items {
			if m.name != lastName {
				typ := "counter"
				if m.gauge {
					typ = "gauge"
				}
				b.WriteString("# TYPE ")
				b.WriteString(m.name)
				b.WriteByte(' ')
				b.WriteString(typ)
				b.WriteByte('\n')
				lastName = m.name
			}
			b.WriteString(m.name)
			if m.labels != "" {
				b.WriteByte('{')
				b.WriteString(m.labels)
				b.WriteByte('}')
			}
			b.WriteByte(' ')
			b.WriteString(strconv.FormatInt(m.value.Load(), 10))
			b.WriteByte('\n')
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(b.String()))
	})
}
