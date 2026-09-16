package main

import (
	"os"
	"regexp"
	"testing"
)

// TestEveryDiskOptionIsWiredIntoMain guards a bug class that unit tests
// structurally cannot see.
//
// iter2 added --shared_storage_verify_interval, --boot_scan_concurrency and
// --gc_concurrency. All three were parsed by config, range-validated, and
// advertised in --help -- and none of them were ever passed to disk.New. The
// flags were accepted and silently ignored. Every unit test passed, because
// they construct a cache by calling disk.WithX(...) directly and so exercise
// the option while skipping the config -> option hand-off entirely.
//
// A dead tuning flag is worse than a missing one: an operator sets
// --shared_storage_verify_interval expecting FindMissingBlobs to stop trusting
// bare index hits, gets no error, and then lowers GC retention believing that
// window is closed. That is the precise sequence the flag exists to prevent.
//
// This test reads the source rather than calling anything, because the defect
// is an absence at a call site. It is deliberately dumb: if an option exists,
// main.go must mention it. Anything intentionally not driven by config belongs
// in notConfigDriven below, with a reason.
func TestEveryDiskOptionIsWiredIntoMain(t *testing.T) {
	// Options that legitimately never appear in the config-driven opts list.
	// Empty today; add entries with a justification rather than deleting the
	// test if it ever fires.
	notConfigDriven := map[string]string{}

	optsSrc, err := os.ReadFile("cache/disk/options.go")
	if err != nil {
		t.Fatalf("read options.go: %v", err)
	}
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}

	defined := regexp.MustCompile(`(?m)^func (With\w+)\(`).FindAllStringSubmatch(string(optsSrc), -1)
	if len(defined) == 0 {
		t.Fatal("found no disk.WithX options in cache/disk/options.go; this " +
			"test's parsing has broken and it is no longer guarding anything")
	}

	for _, m := range defined {
		name := m[1]
		if reason, ok := notConfigDriven[name]; ok {
			t.Logf("skipping %s: %s", name, reason)
			continue
		}
		if !regexp.MustCompile(`\bdisk\.` + name + `\b`).Match(mainSrc) {
			t.Errorf("disk.%s is defined but never referenced in main.go: the "+
				"flag or config field behind it is DEAD -- accepted, validated, "+
				"and silently ignored. Wire it into the opts list in run(), or "+
				"add it to notConfigDriven with a reason.", name)
		}
	}
}
