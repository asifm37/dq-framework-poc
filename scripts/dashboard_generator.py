"""
Dashboard Generator - Creates HTML dashboard with time-series charts
"""
import sys
sys.path.insert(0, '/Users/amohiuddeen/Github/dq-framework-poc')

from scripts.results_tracker import (
    get_recent_results,
    get_table_summary,
    get_hourly_trends,
    init_database
)
from datetime import datetime
import json

DASHBOARD_PATH = "/Users/amohiuddeen/Github/dq-framework-poc/reports/dashboard.html"


def generate_dashboard():
    """Generate HTML dashboard with charts"""
    init_database()
    
    # Get data
    recent_results = get_recent_results(hours=24)
    table_summary = get_table_summary()
    hourly_trends = get_hourly_trends(hours=24)
    
    # Prepare chart data
    chart_data = prepare_chart_data(hourly_trends)
    
    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f7;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            font-size: 32px;
            margin-bottom: 10px;
        }}
        .header p {{
            opacity: 0.9;
            font-size: 14px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }}
        .stat-card h3 {{
            color: #666;
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .value {{
            font-size: 36px;
            font-weight: 700;
            color: #333;
        }}
        .stat-card .sub {{
            color: #999;
            font-size: 13px;
            margin-top: 5px;
        }}
        .chart-container {{
            background: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }}
        .chart-container h2 {{
            margin-bottom: 20px;
            color: #333;
            font-size: 20px;
        }}
        canvas {{
            max-height: 400px;
        }}
        .table-container {{
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            margin-bottom: 30px;
        }}
        .table-header {{
            padding: 20px 30px;
            border-bottom: 1px solid #eee;
        }}
        .table-header h2 {{
            color: #333;
            font-size: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th {{
            background: #f9f9f9;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{
            padding: 15px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 14px;
        }}
        tr:hover {{
            background: #fafafa;
        }}
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }}
        .badge.pass {{
            background: #d4edda;
            color: #155724;
        }}
        .badge.fail {{
            background: #f8d7da;
            color: #721c24;
        }}
        .badge.error {{
            background: #fff3cd;
            color: #856404;
        }}
        .pass-rate {{
            font-weight: 600;
        }}
        .pass-rate.good {{
            color: #28a745;
        }}
        .pass-rate.warning {{
            color: #ffc107;
        }}
        .pass-rate.bad {{
            color: #dc3545;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🛡️ Data Quality Dashboard</h1>
            <p>Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
        
        {generate_stats_cards(table_summary)}
        
        <div class="chart-container">
            <h2>📈 Hourly Pass Rate Trends (Last 24 Hours)</h2>
            <canvas id="trendsChart"></canvas>
        </div>
        
        {generate_summary_table(table_summary)}
        
        {generate_recent_runs_table(recent_results)}
    </div>
    
    <script>
        const chartData = {json.dumps(chart_data)};
        
        // Create line chart
        const ctx = document.getElementById('trendsChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: chartData.labels,
                datasets: chartData.datasets
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{
                            usePointStyle: true,
                            padding: 15
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        max: 100,
                        ticks: {{
                            callback: function(value) {{
                                return value + '%';
                            }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    
    # Write to file
    with open(DASHBOARD_PATH, 'w') as f:
        f.write(html)
    
    print(f"✅ Dashboard generated: {DASHBOARD_PATH}")


def generate_stats_cards(table_summary):
    """Generate summary statistics cards"""
    if not table_summary:
        return '<div class="stat-card"><h3>No Data</h3></div>'
    
    total_tables = len(table_summary)
    avg_pass_rate = sum(t['avg_pass_rate'] or 0 for t in table_summary) / total_tables if total_tables > 0 else 0
    total_runs = sum(t['total_runs'] for t in table_summary)
    healthy_tables = sum(1 for t in table_summary if (t['avg_pass_rate'] or 0) >= 90)
    
    return f"""
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Tables</h3>
                <div class="value">{total_tables}</div>
                <div class="sub">{healthy_tables} healthy (≥90%)</div>
            </div>
            <div class="stat-card">
                <h3>Average Pass Rate</h3>
                <div class="value">{avg_pass_rate:.1f}%</div>
                <div class="sub">Across all tables</div>
            </div>
            <div class="stat-card">
                <h3>Total Validations</h3>
                <div class="value">{total_runs}</div>
                <div class="sub">Last 24 hours</div>
            </div>
            <div class="stat-card">
                <h3>Health Status</h3>
                <div class="value">{"🟢" if avg_pass_rate >= 90 else "🟡" if avg_pass_rate >= 75 else "🔴"}</div>
                <div class="sub">{"Healthy" if avg_pass_rate >= 90 else "Warning" if avg_pass_rate >= 75 else "Critical"}</div>
            </div>
        </div>
    """


def generate_summary_table(table_summary):
    """Generate table summary"""
    if not table_summary:
        return ""
    
    rows = ""
    for t in table_summary:
        pass_rate = t['avg_pass_rate'] or 0
        pass_rate_class = "good" if pass_rate >= 90 else "warning" if pass_rate >= 75 else "bad"
        
        rows += f"""
            <tr>
                <td>{t['table_name']}</td>
                <td>{t['total_runs']}</td>
                <td class="pass-rate {pass_rate_class}">{pass_rate:.2f}%</td>
                <td class="pass-rate">{t['min_pass_rate']:.2f}%</td>
                <td class="pass-rate">{t['max_pass_rate']:.2f}%</td>
                <td>{t['success_count']} / {t['total_runs']}</td>
            </tr>
        """
    
    return f"""
        <div class="table-container">
            <div class="table-header">
                <h2>📊 Table Summary</h2>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Table Name</th>
                        <th>Total Runs</th>
                        <th>Avg Pass Rate</th>
                        <th>Min Pass Rate</th>
                        <th>Max Pass Rate</th>
                        <th>Success Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    """


def generate_recent_runs_table(recent_results):
    """Generate recent runs table"""
    if not recent_results:
        return ""
    
    rows = ""
    for r in recent_results[:50]:  # Limit to 50 most recent
        status_badge = f'<span class="badge {r["status"]}">{r["status"].upper()}</span>'
        pass_rate = r['pass_rate'] or 0
        pass_rate_class = "good" if pass_rate >= 90 else "warning" if pass_rate >= 75 else "bad"
        
        rows += f"""
            <tr>
                <td>{r['run_timestamp'][:19]}</td>
                <td>{r['table_name']}</td>
                <td>{r['test_type']}</td>
                <td>{r['row_count']}</td>
                <td class="pass-rate {pass_rate_class}">{pass_rate:.2f}%</td>
                <td>{status_badge}</td>
            </tr>
        """
    
    return f"""
        <div class="table-container">
            <div class="table-header">
                <h2>🕒 Recent Validation Runs</h2>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Timestamp</th>
                        <th>Table</th>
                        <th>Test Type</th>
                        <th>Rows</th>
                        <th>Pass Rate</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    """


def prepare_chart_data(hourly_trends):
    """Prepare data for Chart.js"""
    if not hourly_trends:
        return {"labels": [], "datasets": []}
    
    # Group by table
    tables = {}
    for trend in hourly_trends:
        table = trend['table_name']
        if table not in tables:
            tables[table] = []
        tables[table].append({
            'hour': trend['hour_bucket'],
            'pass_rate': trend['avg_pass_rate']
        })
    
    # Get unique hours (sorted)
    all_hours = sorted(set(t['hour_bucket'] for t in hourly_trends), reverse=True)
    
    # Create datasets
    colors = [
        '#667eea', '#764ba2', '#f093fb', '#4facfe',
        '#43e97b', '#fa709a', '#fee140', '#30cfd0',
        '#a8edea', '#fed6e3'
    ]
    
    datasets = []
    for idx, (table_name, data) in enumerate(tables.items()):
        # Create mapping
        hour_map = {d['hour']: d['pass_rate'] for d in data}
        values = [hour_map.get(h, None) for h in all_hours]
        
        datasets.append({
            'label': table_name,
            'data': values,
            'borderColor': colors[idx % len(colors)],
            'backgroundColor': colors[idx % len(colors)] + '20',
            'tension': 0.3,
            'fill': False
        })
    
    return {
        'labels': all_hours,
        'datasets': datasets
    }


if __name__ == "__main__":
    generate_dashboard()

