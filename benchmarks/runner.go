//go:build ignore

// runner.go runs gocachemark benchmarks and validates results.
//
// Usage:
//
//	go run benchmarks/runner.go                  # solo multicache, validate hitrate
//	go run benchmarks/runner.go -competitive    # gold medalists, track rankings
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Hitrate goals (averages across all cache sizes).
// Keys must match gocachemark JSON output (camelCase).
var hitrateGoals = map[string]float64{
	"cdn":          58.3,
	"meta":         72.0,
	"twitter":      84.5,
	"wikipedia":    30.59,
	"thesiosBlock": 17.85,
	"thesiosFile":  88.03,
	"ibmDocker":    82.95,
	"tencentPhoto": 19.7,
}

// Gold medalists for competitive benchmarking.
var goldMedalists = "multicache,otter,clock,theine,sieve,freelru-sync"

const (
	minMulticacheScore = 142
	gocachemarkRepo    = "github.com/tstromberg/gocachemark"
	multicacheModule   = "github.com/codeGROOVE-dev/multicache"
)

func main() {
	competitive := flag.Bool("competitive", false, "Run competitive benchmark with gold medalists")
	flag.Parse()

	// Find multicache root (where we're running from).
	multicacheDir, err := findMulticacheDir()
	if err != nil {
		fatal("finding multicache directory: %v", err)
	}

	// Find or clone gocachemark.
	gocachemarkDir, err := findOrCloneGocachemark(multicacheDir)
	if err != nil {
		fatal("finding gocachemark: %v", err)
	}

	// Update go.mod replace directive.
	if err := updateReplace(gocachemarkDir, multicacheDir); err != nil {
		fatal("updating go.mod replace: %v", err)
	}

	// Prepare output directory for results.
	benchmarksDir := filepath.Join(multicacheDir, "benchmarks")

	// Load previous results for comparison (competitive mode).
	var prevResults *Results
	if *competitive {
		prevResults, _ = loadResults(filepath.Join(benchmarksDir, "gocachemark_results.json"))
	}

	// Build gocachemark arguments.
	args := []string{"run", "."}
	var outdir string
	if *competitive {
		args = append(args, "-caches", goldMedalists)
		outdir = benchmarksDir
	} else {
		args = append(args, "-caches", "multicache")
		outdir, err = os.MkdirTemp("", "gocachemark-")
		if err != nil {
			fatal("creating temp directory: %v", err)
		}
		defer os.RemoveAll(outdir)
	}
	args = append(args, "-outdir", outdir)

	// Run gocachemark with streaming output.
	fmt.Printf("Running %s benchmarks via gocachemark...\n\n", modeName(*competitive))
	results, err := runGocachemark(gocachemarkDir, args)
	if err != nil {
		fatal("running gocachemark: %v", err)
	}

	// Validate results.
	fmt.Println()
	if *competitive {
		if err := validateCompetitive(results, prevResults); err != nil {
			fatal("%v", err)
		}
		fmt.Printf("\nResults saved to %s/\n", benchmarksDir)
	} else {
		if err := validateHitrate(results); err != nil {
			fatal("%v", err)
		}
	}
}

func modeName(competitive bool) string {
	if competitive {
		return "competitive"
	}
	return "multicache"
}

func findMulticacheDir() (string, error) {
	// Look for go.mod with multicache module.
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		modPath := filepath.Join(dir, "go.mod")
		if data, err := os.ReadFile(modPath); err == nil {
			if strings.Contains(string(data), multicacheModule) {
				return dir, nil
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("could not find multicache root (no go.mod with %s)", multicacheModule)
}

func findOrCloneGocachemark(multicacheDir string) (string, error) {
	// Check locations in order of preference.
	locations := []string{
		os.Getenv("GOCACHEMARK_DIR"),
		filepath.Join(os.Getenv("HOME"), "src", "gocachemark"),
		filepath.Join(multicacheDir, "out", "gocachemark"),
	}

	for _, loc := range locations {
		if loc == "" {
			continue
		}
		if isGocachemarkDir(loc) {
			return loc, nil
		}
	}

	// Clone to out/gocachemark.
	cloneDir := filepath.Join(multicacheDir, "out", "gocachemark")
	fmt.Printf("Cloning gocachemark to %s...\n", cloneDir)

	if err := os.MkdirAll(filepath.Dir(cloneDir), 0755); err != nil {
		return "", fmt.Errorf("creating out directory: %w", err)
	}

	cmd := exec.Command("git", "clone", "https://"+gocachemarkRepo, cloneDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("cloning gocachemark: %w", err)
	}

	return cloneDir, nil
}

func isGocachemarkDir(dir string) bool {
	mainGo := filepath.Join(dir, "main.go")
	if _, err := os.Stat(mainGo); err != nil {
		return false
	}
	goMod := filepath.Join(dir, "go.mod")
	data, err := os.ReadFile(goMod)
	if err != nil {
		return false
	}
	return strings.Contains(string(data), gocachemarkRepo)
}

func updateReplace(gocachemarkDir, multicacheDir string) error {
	cmd := exec.Command("go", "mod", "edit",
		"-replace", multicacheModule+"="+multicacheDir)
	cmd.Dir = gocachemarkDir
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runGocachemark(dir string, args []string) (*Results, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Stream output to stdout.
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// Find and parse the JSON results.
	var jsonPath string
	for _, arg := range args {
		if strings.HasPrefix(arg, "/") || strings.HasPrefix(arg, "./") {
			candidate := filepath.Join(arg, "gocachemark_results.json")
			if _, err := os.Stat(candidate); err == nil {
				jsonPath = candidate
				break
			}
		}
	}

	// Try to find -outdir argument.
	for i, arg := range args {
		if arg == "-outdir" && i+1 < len(args) {
			jsonPath = filepath.Join(args[i+1], "gocachemark_results.json")
			break
		}
	}

	if jsonPath == "" {
		return nil, fmt.Errorf("could not find results JSON")
	}

	return loadResults(jsonPath)
}

// Results represents gocachemark JSON output.
type Results struct {
	HitRate    map[string]json.RawMessage `json:"hitRate"`
	Rankings   []RankEntry                `json:"rankings"`
	MedalTable MedalTable                 `json:"medalTable"`
}

type MedalTable struct {
	Categories []Category `json:"categories"`
}

type Category struct {
	Name       string      `json:"name"`
	Benchmarks []Benchmark `json:"benchmarks"`
}

type Benchmark struct {
	Name   string `json:"name"`
	Gold   string `json:"gold"`
	Silver string `json:"silver"`
	Bronze string `json:"bronze"`
}

type CacheResult struct {
	Name    string             `json:"name"`
	Rates   map[string]float64 `json:"rates"`
	AvgRate float64            `json:"avgRate"`
}

type RankEntry struct {
	Rank   int    `json:"rank"`
	Name   string `json:"name"`
	Score  int    `json:"score"`
	Gold   int    `json:"gold"`
	Silver int    `json:"silver"`
	Bronze int    `json:"bronze"`
}

// getHitRateResults extracts cache results for a test, skipping non-test fields like "sizes".
func (r *Results) getHitRateResults(testName string) ([]CacheResult, error) {
	raw, ok := r.HitRate[testName]
	if !ok {
		return nil, fmt.Errorf("test %q not found", testName)
	}

	var results []CacheResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, err // Likely "sizes" or other non-test field
	}
	return results, nil
}

func loadResults(path string) (*Results, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results Results
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return &results, nil
}

func validateHitrate(results *Results) error {
	fmt.Println("=== Hitrate Validation ===")

	var failures []string

	for testName, goal := range hitrateGoals {
		cacheResults, err := results.getHitRateResults(testName)
		if err != nil {
			fmt.Printf("? %s: %v\n", testName, err)
			continue
		}

		// Find multicache result.
		var avg float64
		var found bool
		for _, cr := range cacheResults {
			if cr.Name == "multicache" {
				avg = cr.AvgRate
				found = true
				break
			}
		}

		if !found {
			fmt.Printf("? %s: multicache not found\n", testName)
			continue
		}

		// Use small tolerance for floating point comparison (0.01%).
		if avg >= goal-0.01 {
			fmt.Printf("✓ %s: %.2f%% (goal: %.2f%%)\n", testName, avg, goal)
		} else {
			fmt.Printf("✗ %s: %.2f%% (goal: %.2f%%)\n", testName, avg, goal)
			failures = append(failures, fmt.Sprintf("%s: %.2f%% < %.2f%%", testName, avg, goal))
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("hitrate goals not met:\n  %s", strings.Join(failures, "\n  "))
	}

	fmt.Println("\nAll hitrate goals met!")
	return nil
}

func validateCompetitive(results, prevResults *Results) error {
	// Find multicache in rankings.
	var multicacheRank *RankEntry
	for i := range results.Rankings {
		if results.Rankings[i].Name == "multicache" {
			multicacheRank = &results.Rankings[i]
			break
		}
	}

	if multicacheRank == nil {
		return fmt.Errorf("multicache not found in rankings")
	}

	// Report ranking changes if we have previous results.
	if prevResults != nil {
		fmt.Println("=== Ranking Changes ===")
		reportChanges(prevResults, results)
	}

	fmt.Println("\n=== Final Validation ===")

	var failures []string

	// Check minimum score.
	if multicacheRank.Score >= minMulticacheScore {
		fmt.Printf("✓ multicache score: %d (goal: ≥%d)\n", multicacheRank.Score, minMulticacheScore)
	} else {
		fmt.Printf("✗ multicache score: %d (goal: ≥%d)\n", multicacheRank.Score, minMulticacheScore)
		failures = append(failures, fmt.Sprintf("score %d < %d", multicacheRank.Score, minMulticacheScore))
	}

	// Check for point reduction.
	if prevResults != nil {
		var prevScore int
		for _, r := range prevResults.Rankings {
			if r.Name == "multicache" {
				prevScore = r.Score
				break
			}
		}

		if multicacheRank.Score >= prevScore {
			fmt.Printf("✓ No point reduction (was %d, now %d)\n", prevScore, multicacheRank.Score)
		} else {
			fmt.Printf("✗ Point reduction: %d → %d\n", prevScore, multicacheRank.Score)
			failures = append(failures, fmt.Sprintf("points reduced from %d to %d", prevScore, multicacheRank.Score))
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("competitive validation failed:\n  %s", strings.Join(failures, "\n  "))
	}

	return nil
}

func reportChanges(prev, curr *Results) {
	// Build maps of benchmark placements: cache -> benchmark -> placement
	type placement struct {
		medal string // "gold", "silver", "bronze", or ""
		value float64
	}
	prevPlacements := buildPlacementMap(prev)
	currPlacements := buildPlacementMap(curr)

	// Build maps for score comparison.
	prevRanks := make(map[string]RankEntry)
	for _, r := range prev.Rankings {
		prevRanks[r.Name] = r
	}

	for _, r := range curr.Rankings {
		p, ok := prevRanks[r.Name]
		if !ok {
			fmt.Printf("%s: new entry with %d points\n", r.Name, r.Score)
			continue
		}

		delta := r.Score - p.Score
		if delta == 0 {
			continue
		}

		sign := "+"
		if delta < 0 {
			sign = ""
		}
		fmt.Printf("%s: %d → %d (%s%d points)\n", r.Name, p.Score, r.Score, sign, delta)

		// Find which benchmarks changed for this cache.
		prevCache := prevPlacements[r.Name]
		currCache := currPlacements[r.Name]
		for bench, currP := range currCache {
			prevP := prevCache[bench]
			if currP.medal != prevP.medal {
				prevMedal := prevP.medal
				if prevMedal == "" {
					prevMedal = "none"
				}
				currMedal := currP.medal
				if currMedal == "" {
					currMedal = "none"
				}
				if currP.value != 0 && prevP.value != 0 {
					fmt.Printf("  %s: %s → %s (%.2f%% → %.2f%%)\n", bench, prevMedal, currMedal, prevP.value, currP.value)
				} else {
					fmt.Printf("  %s: %s → %s\n", bench, prevMedal, currMedal)
				}
			}
		}
	}
}

// buildPlacementMap builds a map of cache -> benchmark -> {medal, value}.
func buildPlacementMap(r *Results) map[string]map[string]struct {
	medal string
	value float64
} {
	result := make(map[string]map[string]struct {
		medal string
		value float64
	})

	// Initialize maps for all caches.
	for _, rank := range r.Rankings {
		result[rank.Name] = make(map[string]struct {
			medal string
			value float64
		})
	}

	// Map display names to JSON keys for hit rate lookup.
	hitRateKeys := map[string]string{
		"CDN":           "cdn",
		"Meta":          "meta",
		"Zipf":          "zipf",
		"Twitter":       "twitter",
		"Wikipedia":     "wikipedia",
		"Thesios Block": "thesiosBlock",
		"Thesios File":  "thesiosFile",
		"IBM Docker":    "ibmDocker",
		"Tencent Photo": "tencentPhoto",
	}

	for _, cat := range r.MedalTable.Categories {
		for _, bench := range cat.Benchmarks {
			benchName := cat.Name + "/" + bench.Name

			// Get hit rate values if applicable.
			var values map[string]float64
			if cat.Name == "Hit Rate" {
				if key, ok := hitRateKeys[bench.Name]; ok {
					if caches, err := r.getHitRateResults(key); err == nil {
						values = make(map[string]float64)
						for _, c := range caches {
							values[c.Name] = c.AvgRate
						}
					}
				}
			}

			// Record placements.
			for cache := range result {
				var medal string
				switch cache {
				case bench.Gold:
					medal = "gold"
				case bench.Silver:
					medal = "silver"
				case bench.Bronze:
					medal = "bronze"
				}
				val := 0.0
				if values != nil {
					val = values[cache]
				}
				result[cache][benchName] = struct {
					medal string
					value float64
				}{medal, val}
			}
		}
	}

	return result
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
