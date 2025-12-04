module github.com/codeGROOVE-dev/sfcache/persist/valkey

go 1.25.4

require (
	github.com/codeGROOVE-dev/sfcache v0.0.0
	github.com/valkey-io/valkey-go v1.0.51
)

require golang.org/x/sys v0.24.0 // indirect

replace github.com/codeGROOVE-dev/sfcache => ../..
