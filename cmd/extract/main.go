package main

import (
	"fmt"
	"log"

	"github.com/prchen818/ot-col-custom/pkg/util"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	dataset := "media"
	allTraces, _ := util.LoadData(fmt.Sprintf("data/%s/ot_trace.txt", dataset))
	sampledTraces, _ := util.LoadData("data/traces.txt")
	err := util.InitCSV(fmt.Sprintf("data/res/%s-NEW-sample.csv", dataset), []string{"traceId", "decision"})

	if err != nil {
		log.Fatal(err)
	}
	allIds := extractTraceIDs(allTraces)
	sampledIds := extractTraceIDs(sampledTraces)

	for id := range allIds {
		decision := "false"
		if _, ok := sampledIds[id]; ok {
			decision = "true"
		}
		if dataset == "sockshop" || dataset == "media" || dataset == "socialNetwork" {
			id = id[:16]
		}
		util.WriteCSV([]string{id, decision})
	}
}

func extractTraceIDs(traces []ptrace.Traces) map[string]struct{} {
	traceIDSet := make(map[string]struct{})
	for _, trace := range traces {
		resourceSpans := trace.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			rs := resourceSpans.At(i)
			scopeSpans := rs.ScopeSpans()
			for j := 0; j < scopeSpans.Len(); j++ {
				ss := scopeSpans.At(j)
				spans := ss.Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					traceIDHex := span.TraceID().String()
					traceIDSet[traceIDHex] = struct{}{}
				}
			}
		}
	}
	return traceIDSet

}
