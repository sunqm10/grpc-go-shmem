/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package shmsc

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoInternalImports enforces the self-containment invariant that defines
// this module: NO source file may import a google.golang.org/grpc/internal/*
// package (nor any other module's internal package). This is what makes the
// plugin upstreamable and splittable into its own repository. The guard walks
// every .go file (including sub-packages under this module) and fails on any
// forbidden import.
func TestNoInternalImports(t *testing.T) {
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			// Forbid any ".../internal" or ".../internal/..." import path.
			if p == "internal" || strings.HasSuffix(p, "/internal") || strings.Contains(p, "/internal/") {
				t.Errorf("%s imports forbidden internal package %q: this module must be self-contained", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking module sources: %v", err)
	}
}
