module github.com/yourfavoritedev/proglog

go 1.14

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/casbin/casbin v1.9.1
	github.com/cloudflare/cfssl v1.4.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/go.net v0.0.1 // indirect
	github.com/hashicorp/raft v1.3.1
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	github.com/hashicorp/serf v0.9.5
	github.com/jmoiron/sqlx v1.2.0 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lib/pq v1.3.0 // indirect
	github.com/prometheus/client_golang v1.9.0 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.4.0
	github.com/stretchr/testify v1.7.0
	github.com/travisjeffery/go-dynaport v1.0.0
	github.com/tysontate/gommap v0.0.0-20210506040252-ef38c88b18e1
	github.com/zmap/zlint/v3 v3.0.0 // indirect
	go.opencensus.io v0.22.2
	go.uber.org/zap v1.13.0
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013
	google.golang.org/grpc v1.32.0
	google.golang.org/protobuf v1.27.1
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v1.0.0
