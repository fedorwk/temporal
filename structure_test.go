package temporal

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const btreeImport = "github.com/tidwall/btree"

// parsePackageSources parses every non-test .go file of this package.
func parsePackageSources(t *testing.T) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		src, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, p, src, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files[p] = f
	}
	if len(files) == 0 {
		t.Fatal("no package sources found")
	}
	return fset, files
}

// importName returns the local name a file uses for path, or "" if not imported.
func importName(f *ast.File, path string) string {
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if p != path {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return path[strings.LastIndex(path, "/")+1:]
	}
	return ""
}

// T-025 (a), TI-001: only time_index_btree.go imports tidwall/btree.
// Also: no reflect or unsafe in package sources, and no sync.Map anywhere.
func TestStructure_Imports(t *testing.T) {
	fset, files := parsePackageSources(t)
	for name, f := range files {
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			switch {
			case p == btreeImport && name != "time_index_btree.go":
				t.Errorf("%s imports %s; only time_index_btree.go may", name, btreeImport)
			case p == "reflect" || p == "unsafe":
				t.Errorf("%s imports %s", name, p)
			}
		}
		syncName := importName(f, "sync")
		if syncName == "" {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == syncName && sel.Sel.Name == "Map" {
					t.Errorf("%s: sync.Map is not allowed", fset.Position(sel.Pos()))
				}
			}
			return true
		})
	}
	if f, ok := files["time_index_btree.go"]; !ok || importName(f, btreeImport) == "" {
		t.Error("time_index_btree.go does not import tidwall/btree")
	}
}

// T-025 (b), C-002, TI-003, K-011: no time-index or key-index implementation
// declares a mutex or atomic field.
func TestStructure_NoLockFields(t *testing.T) {
	fset, files := parsePackageSources(t)
	checked := 0
	for name, f := range files {
		if !strings.HasPrefix(name, "time_index_") && name != "key_index.go" {
			continue
		}
		checked++
		syncName := importName(f, "sync")
		atomicName := importName(f, "sync/atomic")
		ast.Inspect(f, func(n ast.Node) bool {
			st, ok := n.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				if bad := syncTypeName(field.Type, syncName, atomicName); bad != "" {
					t.Errorf("%s: struct field of type %s", fset.Position(field.Pos()), bad)
				}
			}
			return true
		})
	}
	if checked < 3 { // time_index_btree.go, time_index_none.go, key_index.go
		t.Fatalf("checked only %d index files", checked)
	}
}

// syncTypeName reports a forbidden synchronization type in expr, looking
// through pointers, arrays and slices.
func syncTypeName(expr ast.Expr, syncName, atomicName string) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return syncTypeName(e.X, syncName, atomicName)
	case *ast.ArrayType:
		return syncTypeName(e.Elt, syncName, atomicName)
	case *ast.IndexExpr: // generic instantiation such as atomic.Pointer[T]
		return syncTypeName(e.X, syncName, atomicName)
	case *ast.SelectorExpr:
		id, ok := e.X.(*ast.Ident)
		if !ok {
			return ""
		}
		switch {
		case syncName != "" && id.Name == syncName &&
			(e.Sel.Name == "Mutex" || e.Sel.Name == "RWMutex"):
			return "sync." + e.Sel.Name
		case atomicName != "" && id.Name == atomicName:
			return "atomic." + e.Sel.Name
		}
	}
	return ""
}

// T-025 (c), §13.4: the B-tree is constructed with NoLocks: true, and only
// through NewBTreeGOptions.
func TestStructure_BTreeNoLocks(t *testing.T) {
	fset, files := parsePackageSources(t)
	f := files["time_index_btree.go"]
	if f == nil {
		t.Fatal("time_index_btree.go missing")
	}
	pkg := importName(f, btreeImport)
	found := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		fun := call.Fun
		if ix, ok := fun.(*ast.IndexExpr); ok {
			fun = ix.X
		}
		sel, ok := fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); !ok || id.Name != pkg {
			return true
		}
		switch sel.Sel.Name {
		case "NewBTreeG", "New", "NewMap", "NewSet", "NewBTree", "NewGeneric":
			t.Errorf("%s: %s.%s constructs a locking tree", fset.Position(call.Pos()), pkg, sel.Sel.Name)
		case "NewBTreeGOptions":
			found++
			if len(call.Args) != 2 || !hasNoLocksTrue(call.Args[1]) {
				t.Errorf("%s: NewBTreeGOptions without a literal NoLocks: true", fset.Position(call.Pos()))
			}
		}
		return true
	})
	if found == 0 {
		t.Error("no NewBTreeGOptions call found")
	}
}

func hasNoLocksTrue(expr ast.Expr) bool {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, kok := kv.Key.(*ast.Ident)
		val, vok := kv.Value.(*ast.Ident)
		if kok && vok && key.Name == "NoLocks" && val.Name == "true" {
			return true
		}
	}
	return false
}
