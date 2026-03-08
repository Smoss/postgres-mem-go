// Package engine provides the SQL execution engine with request/response dispatch.
package engine

import (
	"fmt"
	"strings"

	"github.com/lib/pq/oid"
)

// TypeMapping holds PostgreSQL type information.
type TypeMapping struct {
	OID    uint32
	GoType string
}

// Supported PostgreSQL types: int4, int8, text, bool, float8, numeric,
// timestamp, timestamptz, uuid, bytea, jsonb.
// Note: CockroachDB parser normalizes some types (e.g., TEXT -> STRING, INT -> INT8).
var typeRegistry = map[string]TypeMapping{
	"int":     {OID: uint32(oid.T_int4), GoType: "int32"},
	"int4":    {OID: uint32(oid.T_int4), GoType: "int32"},
	"integer": {OID: uint32(oid.T_int4), GoType: "int32"},
	"int8":    {OID: uint32(oid.T_int8), GoType: "int64"},
	"bigint":  {OID: uint32(oid.T_int8), GoType: "int64"},
	"text":    {OID: uint32(oid.T_text), GoType: "string"},
	"string": {
		OID:    uint32(oid.T_text),
		GoType: "string",
	}, // CockroachDB normalizes TEXT to STRING
	"varchar":     {OID: uint32(oid.T_varchar), GoType: "string"},
	"char":        {OID: uint32(oid.T_char), GoType: "string"},
	"bool":        {OID: uint32(oid.T_bool), GoType: "bool"},
	"boolean":     {OID: uint32(oid.T_bool), GoType: "bool"},
	"float8":      {OID: uint32(oid.T_float8), GoType: "float64"},
	"double":      {OID: uint32(oid.T_float8), GoType: "float64"},
	"numeric":     {OID: uint32(oid.T_numeric), GoType: "string"},
	"decimal":     {OID: uint32(oid.T_numeric), GoType: "string"},
	"timestamp":   {OID: uint32(oid.T_timestamp), GoType: "time.Time"},
	"timestamptz": {OID: uint32(oid.T_timestamptz), GoType: "time.Time"},
	"uuid":        {OID: uint32(oid.T_uuid), GoType: "string"},
	"bytea":       {OID: uint32(oid.T_bytea), GoType: "[]byte"},
	"bytes": {
		OID:    uint32(oid.T_bytea),
		GoType: "[]byte",
	}, // CockroachDB normalizes BYTEA to BYTES
	"jsonb": {OID: uint32(oid.T_jsonb), GoType: "string"},
}

// ResolveType converts a PostgreSQL type name to its mapping.
// Type names are case-insensitive.
func ResolveType(typeName string) (TypeMapping, error) {
	lowerName := strings.ToLower(strings.TrimSpace(typeName))
	if mapping, ok := typeRegistry[lowerName]; ok {
		return mapping, nil
	}
	return TypeMapping{}, fmt.Errorf("unknown type: %s", typeName)
}

// IsValidType checks if a type name is supported.
func IsValidType(typeName string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(typeName))
	_, ok := typeRegistry[lowerName]
	return ok
}
