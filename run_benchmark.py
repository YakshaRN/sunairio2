"""Run all sample queries and generate an HTML report with metrics and charts."""
import os
os.environ["PYTHONUNBUFFERED"] = "1"

import json
import time
import sys
import requests

API_URL = "http://localhost:8000/api/query"
QUESTIONS_FILE = "/home/ec2-user/Avahi Sample Queries - Questions Only.md"
OUTPUT_JSON = "/home/ec2-user/forecast-app/benchmark_results.json"
OUTPUT_HTML = "/home/ec2-user/forecast-app/static/benchmark_report.html"

def load_questions(path):
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]

def run_query(question, idx, total):
    print(f"[{idx+1}/{total}] {question[:80]}...")
    t0 = time.time()
    try:
        resp = requests.post(API_URL, json={
            "question": question,
            "session_id": f"bench_{idx}",
        }, timeout=300)
        data = resp.json()
        elapsed = time.time() - t0
        metrics = data.get("metrics") or {}
        result = {
            "idx": idx + 1,
            "question": question,
            "status": "ERROR" if data.get("error") else "OK",
            "answer": data.get("answer", ""),
            "explanation": data.get("explanation", ""),
            "sql": data.get("sql", ""),
            "error": data.get("error"),
            "chart": data.get("chart"),
            "total_time_ms": metrics.get("total_time_ms", round(elapsed * 1000, 1)),
            "query_time_ms": metrics.get("query_time_ms"),
            "row_count": metrics.get("row_count"),
            "data_volume_bytes": metrics.get("data_volume_bytes"),
            "cached": metrics.get("cached", False),
        }
        print(f"    -> {result['status']} | total={result['total_time_ms']/1000:.1f}s | query={result.get('query_time_ms', 0) and result['query_time_ms']/1000 or 0:.1f}s | rows={result['row_count']}")
        return result
    except Exception as e:
        print(f"    -> EXCEPTION: {e}")
        return {
            "idx": idx + 1,
            "question": question,
            "status": "EXCEPTION",
            "answer": str(e),
            "error": str(e),
            "total_time_ms": round((time.time() - t0) * 1000, 1),
        }

def generate_html(results):
    ok = sum(1 for r in results if r["status"] == "OK")
    fail = len(results) - ok
    avg_total = sum(r.get("total_time_ms", 0) for r in results if r["status"] == "OK") / max(ok, 1)
    avg_query = sum(r.get("query_time_ms", 0) or 0 for r in results if r["status"] == "OK") / max(ok, 1)

    rows_html = ""
    chart_scripts = ""
    for r in results:
        status_class = "ok" if r["status"] == "OK" else "fail"
        total_s = r.get("total_time_ms", 0) / 1000
        query_s = (r.get("query_time_ms") or 0) / 1000
        row_count = r.get("row_count", "—") if r.get("row_count") is not None else "—"
        sql_escaped = (r.get("sql") or "—").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        answer_escaped = (r.get("answer") or "—").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        error_html = f'<div class="error-box">{r["error"]}</div>' if r.get("error") else ""
        chart_div = ""
        if r.get("chart") and r["status"] == "OK":
            chart_id = f"chart_{r['idx']}"
            chart_div = f'<div id="{chart_id}" style="width:100%;height:350px;margin-top:10px;"></div>'
            c = r["chart"]
            chart_type = c.get("type", "line")

            traces = []
            y_cols = c.get("y_columns", [])
            y_labels = c.get("y_labels", y_cols)
            for yi, ycol in enumerate(y_cols):
                label = y_labels[yi] if yi < len(y_labels) else ycol
                trace_type = "scatter"
                mode = "lines"
                if chart_type == "bar":
                    trace_type = "bar"
                    mode = None
                elif chart_type == "area":
                    trace_type = "scatter"
                    mode = "lines"
                t = {"x_col": c.get("x_column", ""), "y_col": ycol, "name": label, "type": trace_type}
                if mode:
                    t["mode"] = mode
                if chart_type == "area":
                    t["fill"] = "tozeroy" if yi == 0 else "tonexty"
                traces.append(t)

            chart_scripts += f"""
            (function() {{
                var el = document.getElementById('{chart_id}');
                if (!el) return;
                var data = {json.dumps(r.get('_chart_data', {'columns':[], 'rows':[]}))};
                var cols = data.columns || [];
                var rows = data.rows || [];
                var traces = {json.dumps(traces)};
                var plotTraces = [];
                for (var ti = 0; ti < traces.length; ti++) {{
                    var t = traces[ti];
                    var xi = cols.indexOf(t.x_col);
                    var yi = cols.indexOf(t.y_col);
                    if (xi < 0 || yi < 0) continue;
                    var xvals = rows.map(function(r) {{ return r[xi]; }});
                    var yvals = rows.map(function(r) {{ return r[yi]; }});
                    var pt = {{x: xvals, y: yvals, name: t.name, type: t.type}};
                    if (t.mode) pt.mode = t.mode;
                    if (t.fill) pt.fill = t.fill;
                    plotTraces.push(pt);
                }}
                var layout = {{
                    title: {json.dumps(c.get('title', ''))},
                    xaxis: {{title: {json.dumps(c.get('x_label', ''))}}},
                    yaxis: {{title: {json.dumps(c.get('y_label', ''))}}},
                    template: 'plotly_dark',
                    paper_bgcolor: '#1a1a2e',
                    plot_bgcolor: '#16213e',
                    font: {{color: '#e0e0e0'}},
                    margin: {{t: 40, b: 50, l: 60, r: 20}},
                }};
                Plotly.newPlot(el, plotTraces, layout, {{responsive: true}});
            }})();
            """

        rows_html += f"""
        <div class="query-card {status_class}">
            <div class="query-header">
                <span class="query-num">Q{r['idx']}</span>
                <span class="query-status {status_class}">{r['status']}</span>
                <span class="query-text">{r['question']}</span>
            </div>
            <div class="metrics-row">
                <div class="metric"><span class="label">Total</span><span class="value">{total_s:.1f}s</span></div>
                <div class="metric"><span class="label">Query</span><span class="value">{query_s:.1f}s</span></div>
                <div class="metric"><span class="label">Rows</span><span class="value">{row_count}</span></div>
            </div>
            {error_html}
            <details class="sql-section"><summary>SQL Query</summary><pre>{sql_escaped}</pre></details>
            <details class="answer-section"><summary>Answer</summary><div class="answer-text">{answer_escaped}</div></details>
            {chart_div}
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Forecast Engine Benchmark Report</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f0f1a; color: #e0e0e0; padding: 20px; }}
h1 {{ text-align: center; margin-bottom: 8px; font-size: 24px; color: #a78bfa; }}
.summary {{ display: flex; justify-content: center; gap: 30px; margin-bottom: 24px; padding: 16px; background: #1a1a2e; border-radius: 12px; }}
.summary .stat {{ text-align: center; }}
.summary .stat .num {{ font-size: 28px; font-weight: 700; }}
.summary .stat .lbl {{ font-size: 12px; color: #888; text-transform: uppercase; }}
.stat .num.green {{ color: #4ade80; }}
.stat .num.red {{ color: #f87171; }}
.stat .num.blue {{ color: #60a5fa; }}
.query-card {{ background: #1a1a2e; border-radius: 10px; padding: 16px; margin-bottom: 12px; border-left: 4px solid #4ade80; }}
.query-card.fail {{ border-left-color: #f87171; }}
.query-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }}
.query-num {{ font-weight: 700; color: #a78bfa; min-width: 30px; }}
.query-status {{ font-size: 11px; padding: 2px 8px; border-radius: 4px; font-weight: 600; }}
.query-status.ok {{ background: #064e3b; color: #4ade80; }}
.query-status.fail {{ background: #7f1d1d; color: #f87171; }}
.query-text {{ font-size: 14px; }}
.metrics-row {{ display: flex; gap: 20px; margin-bottom: 8px; }}
.metric .label {{ font-size: 11px; color: #888; display: block; }}
.metric .value {{ font-size: 16px; font-weight: 600; color: #60a5fa; }}
.error-box {{ background: #7f1d1d; color: #fca5a5; padding: 8px 12px; border-radius: 6px; font-size: 13px; margin: 6px 0; }}
details {{ margin-top: 6px; }}
summary {{ cursor: pointer; font-size: 13px; color: #a78bfa; padding: 4px 0; }}
pre {{ background: #16213e; padding: 10px; border-radius: 6px; font-size: 12px; overflow-x: auto; margin-top: 4px; white-space: pre-wrap; }}
.answer-text {{ font-size: 13px; line-height: 1.6; margin-top: 4px; white-space: pre-wrap; }}
</style>
</head>
<body>
<h1>Forecast Engine — Benchmark Report</h1>
<p style="text-align:center;color:#888;margin-bottom:16px;">50 Avahi Sample Queries — {time.strftime('%Y-%m-%d %H:%M UTC')}</p>
<div class="summary">
    <div class="stat"><div class="num green">{ok}</div><div class="lbl">Passed</div></div>
    <div class="stat"><div class="num red">{fail}</div><div class="lbl">Failed</div></div>
    <div class="stat"><div class="num blue">{avg_total/1000:.1f}s</div><div class="lbl">Avg Total</div></div>
    <div class="stat"><div class="num blue">{avg_query/1000:.1f}s</div><div class="lbl">Avg Query</div></div>
</div>
{rows_html}
<script>
{chart_scripts}
</script>
</body>
</html>"""
    return html


def main():
    questions = load_questions(QUESTIONS_FILE)
    print(f"Loaded {len(questions)} questions\n")

    results = []
    for i, q in enumerate(questions):
        result = run_query(q, i, len(questions))
        results.append(result)
        time.sleep(1)

    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {OUTPUT_JSON}")

    # Reload with chart data for HTML generation
    for r in results:
        if r.get("chart") and r["status"] == "OK":
            # Re-fetch just data for chart rendering (from cached result hopefully)
            try:
                resp = requests.post(API_URL, json={"question": r["question"], "session_id": f"bench_{r['idx']-1}"}, timeout=300)
                data = resp.json()
                if data.get("data"):
                    r["_chart_data"] = {"columns": data["data"].get("columns", []), "rows": data["data"].get("rows", [])}
            except:
                r["_chart_data"] = {"columns": [], "rows": []}

    html = generate_html(results)
    with open(OUTPUT_HTML, "w") as f:
        f.write(html)
    print(f"HTML report saved to {OUTPUT_HTML}")
    print(f"\nDone! View at: http://<host>:8000/static/benchmark_report.html")


if __name__ == "__main__":
    main()

