// Command vgi-kubernetes-worker serves the kubernetes catalog to DuckDB.
//
//	vgi-kubernetes-worker                                  # stdio (what DuckDB launches)
//	vgi-kubernetes-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-kubernetes-worker --unix /tmp/kubernetes.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'kubernetes' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-kubernetes-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/your-org/vgi-kubernetes/internal/kubernetes"
	"github.com/your-org/vgi-kubernetes/internal/serve"
)

func main() {
	serve.Main("vgi-kubernetes-worker", os.Args[1:], func() *vgi.Worker { return kubernetes.NewWorker() })
}
