package server_test

import (
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Matthew-Klette/inference-queue/api"
	"github.com/Matthew-Klette/inference-queue/internal/server"
)

// The OpenAPI document and the router are maintained by hand, so this test is
// the guard that keeps them from drifting: every registered route must be in
// the spec and every operation in the spec must be routed.
func TestOpenAPISpecMatchesRoutes(t *testing.T) {
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(api.Spec, &doc); err != nil {
		t.Fatalf("spec is not valid YAML: %v", err)
	}
	methods := map[string]bool{"get": true, "post": true, "put": true, "patch": true, "delete": true}
	var fromSpec []string
	for path, ops := range doc.Paths {
		for method := range ops {
			if methods[method] {
				fromSpec = append(fromSpec, strings.ToUpper(method)+" "+path)
			}
		}
	}
	sort.Strings(fromSpec)

	s := &server.Server{}
	s.Handler()
	fromRouter := s.Routes()

	if strings.Join(fromSpec, "\n") != strings.Join(fromRouter, "\n") {
		t.Fatalf("spec and router disagree\nspec:\n  %s\nrouter:\n  %s",
			strings.Join(fromSpec, "\n  "), strings.Join(fromRouter, "\n  "))
	}
}
