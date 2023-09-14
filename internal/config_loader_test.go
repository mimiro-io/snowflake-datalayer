package internal

import "testing"

func TestLoadConfig(t *testing.T) {
	c := &ConfigLoader{}
	//LoadLogger("console", "test", "debug")
	c.loadConfig = c.loadFile
	cfg := &Config{ConfigLocation: "../testdata/config.json"}
	res := c.update(cfg)
	if !res {
		t.Fatal("Expected config to be updated")
	}
	if len(cfg.DsMappings) != 1 {
		t.Fatal("Expected 1 mapping")
	}
	res = c.update(cfg)
	if res {
		t.Fatal("Expected config to be unchanged")
	}
	if len(cfg.DsMappings) != 1 {
		t.Fatal("Still expected 1 mapping")
	}
	if cfg.DsMappings[0].DatasetName != "huzzah" {
		t.Fatal("Expected name to be test")
	}

	cfg.ConfigLocation = "../testdata/config2.json"
	res = c.update(cfg)
	if !res {
		t.Fatal("Expected config to be updated")
	}
	if len(cfg.DsMappings) != 2 {
		t.Fatal("Expected 2 mappings")
	}
}