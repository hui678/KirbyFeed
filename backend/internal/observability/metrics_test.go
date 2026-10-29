package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerRendersCountersAndGauges(t *testing.T) {
	Inc("test_events_total", "queue", "like.events")
	Add("test_events_total", 2, "queue", "like.events")
	SetGauge("test_queue_depth", 7, "queue", "like.events")

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"# TYPE test_events_total counter",
		`test_events_total{queue="like.events"} 3`,
		"# TYPE test_queue_depth gauge",
		`test_queue_depth{queue="like.events"} 7`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
	if strings.Count(body, "# TYPE test_events_total") != 1 {
		t.Fatalf("TYPE line must be emitted once per metric family:\n%s", body)
	}
}
