#!/usr/bin/env python3
"""
Generate PNG performance graphs for DDB v2 benchmark results
Using actual data from benchmark_results_final.txt
"""
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import numpy as np

# Set style for professional-looking graphs
plt.style.use('seaborn-v0_8-darkgrid' if hasattr(plt.style, 'available') and 'seaborn-v0_8-darkgrid' in plt.style.available else 'default')

# Actual benchmark data from criterion results
# All times converted to microseconds (µs) for consistency

def format_time_human_readable(microseconds):
    """
    Convert microseconds to human-readable format with multiple units.

    Examples:
        0.5 µs -> "0.5 µs (0.0000005 sec)"
        1000 µs -> "1000 µs (1.0 ms / 0.001 sec)"
        10000 µs -> "10000 µs (10.0 ms / 0.01 sec)"
    """
    seconds = microseconds / 1_000_000

    if microseconds < 1:
        # Nanoseconds range
        nanoseconds = microseconds * 1000
        return f"{nanoseconds:.0f} ns\n({seconds:.9f} sec)"
    elif microseconds < 1000:
        # Microseconds range (no milliseconds conversion needed)
        return f"{microseconds:.1f} µs\n({seconds:.6f} sec)"
    elif microseconds < 10000:
        # Show milliseconds and seconds
        milliseconds = microseconds / 1000
        return f"{microseconds:.0f} µs\n({milliseconds:.2f} ms / {seconds:.4f} sec)"
    else:
        # Large values - show all three
        milliseconds = microseconds / 1000
        return f"{microseconds:.0f} µs\n({milliseconds:.1f} ms / {seconds:.3f} sec)"

def create_tokenization_graph():
    """Tokenization performance - showing parser speed"""
    operations = ['Simple\nSELECT', 'Complex\nSELECT', 'INSERT', 'UPDATE', 'DELETE']
    times = [0.529, 4.46, 1.43, 0.834, 0.485]  # µs

    fig, ax = plt.subplots(figsize=(14, 8))
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6']
    bars = ax.bar(operations, times, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

    # Add value labels on bars with human-readable format
    for bar, time in zip(bars, times):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                format_time_human_readable(time),
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_ylabel('Time (microseconds)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - SQL Tokenization Performance', fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, max(times) * 1.5)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/tokenization_performance.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/tokenization_performance.png")
    plt.close()

def create_select_performance_graph():
    """SELECT query performance across different row counts"""
    datasets = ['100 rows', '1000 rows', '10000 rows']
    full_scan = [90.1, 801.6, 8096.4]  # µs
    where_filter = [69.6, 618.5, 6230.7]  # µs
    order_by = [94.5, 851.0, 8449.3]  # µs

    x = np.arange(len(datasets))
    width = 0.25

    fig, ax = plt.subplots(figsize=(14, 8))

    bars1 = ax.bar(x - width, full_scan, width, label='Full Scan', color='#3498db', alpha=0.8, edgecolor='black')
    bars2 = ax.bar(x, where_filter, width, label='WHERE Filter', color='#2ecc71', alpha=0.8, edgecolor='black')
    bars3 = ax.bar(x + width, order_by, width, label='ORDER BY', color='#e74c3c', alpha=0.8, edgecolor='black')

    # Add value labels with human-readable format
    for bars, values in [(bars1, full_scan), (bars2, where_filter), (bars3, order_by)]:
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    format_time_human_readable(val),
                    ha='center', va='bottom', fontsize=8, fontweight='bold')

    ax.set_xlabel('Dataset Size', fontsize=14, fontweight='bold')
    ax.set_ylabel('Time (microseconds)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - SELECT Query Performance', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(datasets)
    ax.legend(fontsize=12, loc='upper left')
    ax.set_ylim(0, max(order_by) * 1.35)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/select_performance.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/select_performance.png")
    plt.close()

def create_aggregation_graph():
    """Aggregation performance"""
    operations = ['COUNT\n(100 rows)', 'SUM/AVG\n(100 rows)', 'COUNT\n(1K rows)', 'COUNT\n(10K rows)']
    times = [61.5, 68.1, 527.9, 5279.0]  # µs

    fig, ax = plt.subplots(figsize=(14, 8))
    colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
    bars = ax.bar(operations, times, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

    # Add value labels with human-readable format
    for bar, time in zip(bars, times):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                format_time_human_readable(time),
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_ylabel('Time (microseconds)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - Aggregation Performance', fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, max(times) * 1.4)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/aggregation_performance.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/aggregation_performance.png")
    plt.close()

def create_join_performance_graph():
    """JOIN operation performance"""
    datasets = ['100 rows', '500 rows', '1000 rows']
    inner_join = [63.8, 275.8, 549.8]  # µs
    left_join = [63.7, 276.8, 558.9]  # µs

    x = np.arange(len(datasets))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 8))

    bars1 = ax.bar(x - width/2, inner_join, width, label='INNER JOIN', color='#3498db', alpha=0.8, edgecolor='black')
    bars2 = ax.bar(x + width/2, left_join, width, label='LEFT JOIN', color='#2ecc71', alpha=0.8, edgecolor='black')

    # Add value labels with human-readable format
    for bars, values in [(bars1, inner_join), (bars2, left_join)]:
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    format_time_human_readable(val),
                    ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.set_xlabel('Dataset Size (per table)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Time (microseconds)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - JOIN Performance (with Hash Index Optimization)', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(datasets)
    ax.legend(fontsize=12)
    ax.set_ylim(0, max(left_join) * 1.35)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/join_performance.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/join_performance.png")
    plt.close()

def create_write_operations_graph():
    """INSERT, UPDATE, DELETE performance"""
    operations = ['INSERT\n(1 row)', 'INSERT\n(10 rows)', 'INSERT\n(100 rows)',
                  'UPDATE\n(100 rows)', 'DELETE\n(100 rows)']
    times = [13.4, 16.0, 37.9, 74.3, 78.7]  # µs

    fig, ax = plt.subplots(figsize=(14, 8))
    colors = ['#2ecc71', '#27ae60', '#229954', '#f39c12', '#e74c3c']
    bars = ax.bar(operations, times, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

    # Add value labels with human-readable format
    for bar, time in zip(bars, times):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                format_time_human_readable(time),
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_ylabel('Time (microseconds)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - Write Operations Performance', fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, max(times) * 1.5)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmarks/write_operations.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/write_operations.png")
    plt.close()

def create_optimization_impact_graph():
    """Show the impact of the 4 major optimizations"""
    optimizations = ['Heap-based\nLIMIT', 'Hash Index\nJOIN', 'Memory-mapped\nI/O', 'Parallel\nAggregation']
    improvements = [9.5, 100, 150, 250]  # Percentage improvement (min values)
    colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.bar(optimizations, improvements, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

    # Add value labels
    labels = ['9.5% faster\n(ORDER BY LIMIT)', '100-1000x faster\n(O(n+m) vs O(n×m))',
              '2-3x faster\n(files ≥10MB)', '2-4x faster\n(multi-core)']
    for bar, improvement, label in zip(bars, improvements, labels):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                label,
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_ylabel('Performance Improvement (%)', fontsize=14, fontweight='bold')
    ax.set_title('DDB v2 - Performance Optimization Impact', fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, max(improvements) * 1.3)
    ax.grid(axis='y', alpha=0.3)

    # Add note
    ax.text(0.5, -0.15, 'Note: Hash Index JOIN shows minimum 100% (2x) improvement; actual improvement ranges from 100-1000x for large datasets',
            transform=ax.transAxes, ha='center', fontsize=10, style='italic', wrap=True)

    plt.tight_layout()
    plt.savefig('benchmarks/optimization_impact.png', dpi=300, bbox_inches='tight')
    print("✓ Generated: benchmarks/optimization_impact.png")
    plt.close()

def main():
    """Generate all benchmark graphs"""
    print("\n" + "="*60)
    print("DDB v2 - Benchmark Graph Generation")
    print("="*60 + "\n")

    # Create benchmarks directory if it doesn't exist
    import os
    os.makedirs('benchmarks', exist_ok=True)

    print("Generating performance graphs...\n")

    try:
        create_tokenization_graph()
        create_select_performance_graph()
        create_aggregation_graph()
        create_join_performance_graph()
        create_write_operations_graph()
        create_optimization_impact_graph()

        print("\n" + "="*60)
        print("✓ All 6 benchmark graphs generated successfully!")
        print("="*60 + "\n")
        print("Files created in benchmarks/ directory:")
        print("  - tokenization_performance.png")
        print("  - select_performance.png")
        print("  - aggregation_performance.png")
        print("  - join_performance.png")
        print("  - write_operations.png")
        print("  - optimization_impact.png")
        print()

    except Exception as e:
        print(f"\n✗ Error generating graphs: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0

if __name__ == '__main__':
    exit(main())
