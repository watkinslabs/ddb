#!/usr/bin/env python3
"""
Structured benchmark runner for DDB-Rust
Outputs verifiable JSON logs for analysis
"""

import subprocess
import json
import time
import sys
import os
from datetime import datetime
import platform

# Benchmark configuration
DDB_BINARY = "./target/release/ddb"
NUM_ITERATIONS = 3  # Run each test 3 times for consistency
OUTPUT_FILE = "benchmarks/results/benchmark_results.json"

# Test queries
BENCHMARKS = [
    {
        "name": "count_all",
        "category": "aggregation",
        "query": "SELECT COUNT(*) FROM benchmark_data",
        "description": "Count all 2M rows"
    },
    {
        "name": "filter_region",
        "category": "filtering",
        "query": "SELECT * FROM benchmark_data WHERE region = 'West' LIMIT 1000",
        "description": "Filter by region with LIMIT"
    },
    {
        "name": "filter_price_range",
        "category": "filtering",
        "query": "SELECT * FROM benchmark_data WHERE price > 500 AND price < 600 LIMIT 1000",
        "description": "Filter by price range"
    },
    {
        "name": "like_prefix",
        "category": "pattern_matching",
        "query": "SELECT * FROM benchmark_data WHERE customer_name LIKE 'John%' LIMIT 1000",
        "description": "LIKE prefix match (optimized)"
    },
    {
        "name": "like_suffix",
        "category": "pattern_matching",
        "query": "SELECT * FROM benchmark_data WHERE customer_name LIKE '%son' LIMIT 1000",
        "description": "LIKE suffix match (optimized)"
    },
    {
        "name": "like_contains",
        "category": "pattern_matching",
        "query": "SELECT * FROM benchmark_data WHERE customer_name LIKE '%John%' LIMIT 1000",
        "description": "LIKE contains match (optimized)"
    },
    {
        "name": "like_complex",
        "category": "pattern_matching",
        "query": "SELECT * FROM benchmark_data WHERE customer_name LIKE 'J_hn%' LIMIT 1000",
        "description": "LIKE with underscore wildcard (regex)"
    },
    {
        "name": "sort_price",
        "category": "sorting",
        "query": "SELECT * FROM benchmark_data ORDER BY price DESC LIMIT 100",
        "description": "Sort by price descending"
    },
    {
        "name": "large_result_10k",
        "category": "scanning",
        "query": "SELECT * FROM benchmark_data LIMIT 10000",
        "description": "Read 10K rows"
    },
    {
        "name": "large_result_100k",
        "category": "scanning",
        "query": "SELECT * FROM benchmark_data LIMIT 100000",
        "description": "Read 100K rows"
    },
]

def get_system_info():
    """Collect system information"""
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "timestamp": datetime.now().isoformat(),
    }

def run_single_benchmark(test_name, query, iteration):
    """Run a single benchmark and return timing"""
    print(f"  Running iteration {iteration}...", end=" ", flush=True)

    start_time = time.perf_counter()

    try:
        result = subprocess.run(
            [DDB_BINARY, "--query", query],
            capture_output=True,
            text=True,
            timeout=120
        )

        end_time = time.perf_counter()
        duration = end_time - start_time

        # Count output lines (rough estimate of rows)
        output_lines = len(result.stdout.split('\n'))

        success = result.returncode == 0

        print(f"{duration:.3f}s", end="")
        if success:
            print(" ✓")
        else:
            print(" ✗")

        return {
            "iteration": iteration,
            "duration_seconds": duration,
            "success": success,
            "returncode": result.returncode,
            "output_lines": output_lines,
            "stderr": result.stderr if result.stderr else None
        }

    except subprocess.TimeoutExpired:
        print("TIMEOUT")
        return {
            "iteration": iteration,
            "duration_seconds": None,
            "success": False,
            "returncode": -1,
            "output_lines": 0,
            "stderr": "Timeout after 120 seconds"
        }
    except Exception as e:
        print(f"ERROR: {e}")
        return {
            "iteration": iteration,
            "duration_seconds": None,
            "success": False,
            "returncode": -1,
            "output_lines": 0,
            "stderr": str(e)
        }

def run_benchmarks():
    """Run all benchmarks and collect results"""

    print("╔════════════════════════════════════════════════════════════════╗")
    print("║         DDB-Rust Structured Benchmark Runner                  ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()

    # Check if binary exists
    if not os.path.exists(DDB_BINARY):
        print(f"❌ Error: Binary not found at {DDB_BINARY}")
        print("   Run: cargo build --release")
        sys.exit(1)

    # Check if benchmark data exists
    if not os.path.exists("benchmarks/data/benchmark_data.csv"):
        print("❌ Error: Benchmark data not found")
        print("   Run: python3 benchmarks/scripts/generate_benchmark_data.py")
        sys.exit(1)

    print(f"📊 Running {len(BENCHMARKS)} benchmark tests")
    print(f"🔁 {NUM_ITERATIONS} iterations per test")
    print(f"📝 Results will be saved to: {OUTPUT_FILE}")
    print()

    results = {
        "system_info": get_system_info(),
        "binary": DDB_BINARY,
        "dataset": {
            "rows": 2_000_000,
            "file": "benchmarks/data/benchmark_data.csv",
            "size_mb": os.path.getsize("benchmarks/data/benchmark_data.csv") / (1024 * 1024)
        },
        "iterations_per_test": NUM_ITERATIONS,
        "benchmarks": []
    }

    for idx, benchmark in enumerate(BENCHMARKS, 1):
        print(f"[{idx}/{len(BENCHMARKS)}] {benchmark['name']}: {benchmark['description']}")

        iterations = []
        for i in range(1, NUM_ITERATIONS + 1):
            result = run_single_benchmark(benchmark['name'], benchmark['query'], i)
            iterations.append(result)
            time.sleep(0.5)  # Brief pause between iterations

        # Calculate statistics
        successful_runs = [r for r in iterations if r['success'] and r['duration_seconds'] is not None]

        if successful_runs:
            durations = [r['duration_seconds'] for r in successful_runs]
            stats = {
                "min": min(durations),
                "max": max(durations),
                "mean": sum(durations) / len(durations),
                "median": sorted(durations)[len(durations) // 2]
            }
            print(f"  Stats: min={stats['min']:.3f}s, mean={stats['mean']:.3f}s, max={stats['max']:.3f}s")
        else:
            stats = None
            print("  ⚠️  All iterations failed")

        results["benchmarks"].append({
            "name": benchmark["name"],
            "category": benchmark["category"],
            "query": benchmark["query"],
            "description": benchmark["description"],
            "iterations": iterations,
            "statistics": stats
        })

        print()

    # Save results to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"✅ Benchmark complete! Results saved to: {OUTPUT_FILE}")
    print(f"📊 Total benchmarks: {len(BENCHMARKS)}")
    print(f"🔁 Total test runs: {len(BENCHMARKS) * NUM_ITERATIONS}")
    print()
    print("Next steps:")
    print(f"  1. View results: cat {OUTPUT_FILE}")
    print(f"  2. Generate report: python3 benchmarks/scripts/analyze_benchmarks.py")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == "__main__":
    run_benchmarks()
