module github.com/btcsuite/btcd

require (
	github.com/btcsuite/btcd/btcec/v2 v2.1.3
	github.com/btcsuite/btcd/btcutil v1.1.0
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.1
	github.com/btcsuite/btclog v0.0.0-20170628155309-84c8d2346e9f
	github.com/btcsuite/go-socks v0.0.0-20170105172521-4720035b7bfd
	github.com/btcsuite/websocket v0.0.0-20150119174127-31079b680792
	github.com/btcsuite/winsvc v1.0.0
	github.com/davecgh/go-spew v1.1.1
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1
	github.com/decred/dcrd/lru v1.0.0
	github.com/jessevdk/go-flags v1.4.0
	github.com/jrick/logrotate v1.0.0
	github.com/stretchr/testify v1.7.1
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8
)

require (
	github.com/ledgerwatch/erigon-lib v0.0.0-20220710110825-21c6baf2871c
	github.com/ledgerwatch/log/v3 v3.4.1
	github.com/sirupsen/logrus v1.9.0
	github.com/syndtr/goleveldb v1.0.0
)

require (
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/aead/siphash v1.0.1 // indirect
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/decred/dcrd/crypto/blake256 v1.0.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/kkdai/bstream v0.0.0-20161212061736-f391b8402d23 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/pkg/errors v0.8.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/torquem-ch/mdbx-go v0.24.3-0.20220614090901-342411560dde // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace github.com/btcsuite/btcd/btcutil => ./btcutil

// The retract statements below fixes an accidental push of the tags of a btcd
// fork.
retract (
	v0.18.1
	v0.18.0
	v0.17.1
	v0.17.0
	v0.16.5
	v0.16.4
	v0.16.3
	v0.16.2
	v0.16.1
	v0.16.0

	v0.15.2
	v0.15.1
	v0.15.0

	v0.14.7
	v0.14.6
	v0.14.6
	v0.14.5
	v0.14.4
	v0.14.3
	v0.14.2
	v0.14.1

	v0.14.0
	v0.13.0-beta2
	v0.13.0-beta
)

go 1.17
