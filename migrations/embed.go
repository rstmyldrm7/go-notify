// Package migrations embeds the versioned SQL schema files so services can
// run pending migrations at startup without external tooling.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
