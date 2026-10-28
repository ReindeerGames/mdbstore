package mdbstore

import "testing"

func TestConfigZeroAndExplicitZeroSemantics(t *testing.T) {
	if got := (Config{}).normalized(); got.MaxOpenConns != defaultConfig.MaxOpenConns || got.MaxIdleConns != defaultConfig.MaxIdleConns {
		t.Fatalf("zero config = %+v", got)
	}
	config := NewDefaultConfig()
	config.MaxIdleConns = 0
	config.ConnMaxLifetime = 0
	config.ConnMaxIdleTime = 0
	got := config.normalized()
	if got.MaxIdleConns != 0 || got.ConnMaxLifetime != 0 || got.ConnMaxIdleTime != 0 {
		t.Fatalf("explicit zero config = %+v", got)
	}
}
