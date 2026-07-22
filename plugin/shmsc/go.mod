module google.golang.org/grpc/plugin/shmsc

go 1.25.0

// Monorepo dev override: build the self-contained plugin against the local
// grpc-go tree (which carries the experimental/transport API). This keeps the
// nested module invisible to the root module's `go ./...` and CI (zero impact on
// existing builds). It MUST be replaced with a pinned google.golang.org/grpc
// version when this module is published / split into its own repository.
replace google.golang.org/grpc => ../..

require (
	golang.org/x/net v0.53.0
	golang.org/x/sys v0.43.0
	google.golang.org/grpc v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)
