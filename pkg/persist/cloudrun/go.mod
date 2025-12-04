module github.com/codeGROOVE-dev/sfcache/pkg/persist/cloudrun

go 1.25.4

require (
	github.com/codeGROOVE-dev/sfcache/pkg/persist v0.0.0
	github.com/codeGROOVE-dev/sfcache/pkg/persist/datastore v0.0.0
	github.com/codeGROOVE-dev/sfcache/pkg/persist/localfs v0.0.0
)

require github.com/codeGROOVE-dev/ds9 v0.7.1 // indirect

replace github.com/codeGROOVE-dev/sfcache/pkg/persist => ..

replace github.com/codeGROOVE-dev/sfcache/pkg/persist/datastore => ../datastore

replace github.com/codeGROOVE-dev/sfcache/pkg/persist/localfs => ../localfs
