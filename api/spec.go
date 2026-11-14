// Package api embeds the OpenAPI document served at /openapi.yaml.
package api

import _ "embed"

// Spec is the OpenAPI 3.0 description of the HTTP API. A test in
// internal/server fails if the registered routes and this file disagree.
//
//go:embed openapi.yaml
var Spec []byte
