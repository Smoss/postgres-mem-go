package engine

import (
	"testing"

	"github.com/lib/pq/oid"
)

// @test ResolveType maps int4 to correct OID and Go type
func TestResolveTypeInt4(t *testing.T) {
	mapping, err := ResolveType("int4")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_int4) {
		t.Fatalf("Expected OID %d, got %d", oid.T_int4, mapping.OID)
	}

	if mapping.GoType != "int32" {
		t.Fatalf("Expected Go type 'int32', got %s", mapping.GoType)
	}
}

// @test ResolveType maps int8 to correct OID and Go type
func TestResolveTypeInt8(t *testing.T) {
	mapping, err := ResolveType("int8")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_int8) {
		t.Fatalf("Expected OID %d, got %d", oid.T_int8, mapping.OID)
	}

	if mapping.GoType != "int64" {
		t.Fatalf("Expected Go type 'int64', got %s", mapping.GoType)
	}
}

// @test ResolveType maps text to correct OID and Go type
func TestResolveTypeText(t *testing.T) {
	mapping, err := ResolveType("text")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_text) {
		t.Fatalf("Expected OID %d, got %d", oid.T_text, mapping.OID)
	}

	if mapping.GoType != "string" {
		t.Fatalf("Expected Go type 'string', got %s", mapping.GoType)
	}
}

// @test ResolveType maps bool to correct OID and Go type
func TestResolveTypeBool(t *testing.T) {
	mapping, err := ResolveType("bool")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_bool) {
		t.Fatalf("Expected OID %d, got %d", oid.T_bool, mapping.OID)
	}

	if mapping.GoType != "bool" {
		t.Fatalf("Expected Go type 'bool', got %s", mapping.GoType)
	}
}

// @test ResolveType maps float8 to correct OID and Go type
func TestResolveTypeFloat8(t *testing.T) {
	mapping, err := ResolveType("float8")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_float8) {
		t.Fatalf("Expected OID %d, got %d", oid.T_float8, mapping.OID)
	}

	if mapping.GoType != "float64" {
		t.Fatalf("Expected Go type 'float64', got %s", mapping.GoType)
	}
}

// @test ResolveType maps numeric to correct OID and Go type
func TestResolveTypeNumeric(t *testing.T) {
	mapping, err := ResolveType("numeric")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_numeric) {
		t.Fatalf("Expected OID %d, got %d", oid.T_numeric, mapping.OID)
	}

	if mapping.GoType != "string" {
		t.Fatalf("Expected Go type 'string', got %s", mapping.GoType)
	}
}

// @test ResolveType maps timestamp to correct OID and Go type
func TestResolveTypeTimestamp(t *testing.T) {
	mapping, err := ResolveType("timestamp")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_timestamp) {
		t.Fatalf("Expected OID %d, got %d", oid.T_timestamp, mapping.OID)
	}

	if mapping.GoType != "time.Time" {
		t.Fatalf("Expected Go type 'time.Time', got %s", mapping.GoType)
	}
}

// @test ResolveType maps timestamptz to correct OID and Go type
func TestResolveTypeTimestamptz(t *testing.T) {
	mapping, err := ResolveType("timestamptz")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_timestamptz) {
		t.Fatalf("Expected OID %d, got %d", oid.T_timestamptz, mapping.OID)
	}

	if mapping.GoType != "time.Time" {
		t.Fatalf("Expected Go type 'time.Time', got %s", mapping.GoType)
	}
}

// @test ResolveType maps uuid to correct OID and Go type
func TestResolveTypeUUID(t *testing.T) {
	mapping, err := ResolveType("uuid")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_uuid) {
		t.Fatalf("Expected OID %d, got %d", oid.T_uuid, mapping.OID)
	}

	if mapping.GoType != "string" {
		t.Fatalf("Expected Go type 'string', got %s", mapping.GoType)
	}
}

// @test ResolveType maps bytea to correct OID and Go type
func TestResolveTypeBytea(t *testing.T) {
	mapping, err := ResolveType("bytea")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_bytea) {
		t.Fatalf("Expected OID %d, got %d", oid.T_bytea, mapping.OID)
	}

	if mapping.GoType != "[]byte" {
		t.Fatalf("Expected Go type '[]byte', got %s", mapping.GoType)
	}
}

// @test ResolveType maps jsonb to correct OID and Go type
func TestResolveTypeJSONB(t *testing.T) {
	mapping, err := ResolveType("jsonb")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if mapping.OID != uint32(oid.T_jsonb) {
		t.Fatalf("Expected OID %d, got %d", oid.T_jsonb, mapping.OID)
	}

	if mapping.GoType != "string" {
		t.Fatalf("Expected Go type 'string', got %s", mapping.GoType)
	}
}

// @test ResolveType is case-insensitive
func TestResolveTypeCaseInsensitive(t *testing.T) {
	mappings := []string{"INT4", "Int4", "int4", "INT"}

	for _, typeName := range mappings {
		mapping, err := ResolveType(typeName)
		if err != nil {
			t.Fatalf("Expected no error for %s, got %v", typeName, err)
		}
		if mapping.OID != uint32(oid.T_int4) {
			t.Fatalf(
				"Expected OID %d for %s, got %d",
				oid.T_int4,
				typeName,
				mapping.OID,
			)
		}
	}
}

// @test ResolveType returns error for unknown type
func TestResolveTypeInvalid(t *testing.T) {
	_, err := ResolveType("unknown_type")
	if err == nil {
		t.Fatal("Expected error for unknown type, got nil")
	}

	expected := "unknown type: unknown_type"
	if err.Error() != expected {
		t.Fatalf("Expected error '%s', got '%s'", expected, err.Error())
	}
}

// @test IsValidType returns true for valid types
func TestIsValidType(t *testing.T) {
	validTypes := []string{
		"int4",
		"int8",
		"text",
		"bool",
		"float8",
		"numeric",
		"timestamp",
		"uuid",
		"bytea",
		"jsonb",
	}

	for _, typeName := range validTypes {
		if !IsValidType(typeName) {
			t.Fatalf("Expected %s to be valid", typeName)
		}
	}
}

// @test IsValidType returns false for invalid types
func TestIsValidTypeInvalid(t *testing.T) {
	if IsValidType("invalid_type") {
		t.Fatal("Expected 'invalid_type' to be invalid")
	}
}
