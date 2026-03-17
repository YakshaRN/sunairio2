import json, time, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
results = json.load(open('benchmark_results.json'))
ok = sum(1 for r in results if r.get('status') == 'OK')
fail = len(results) - ok
avg_t = sum(r.get('total_time_ms', 0) for r in results if r.get('status') == 'OK') / max(ok, 1)
avg_q = sum(r.get('query_time_ms', 0) or 0 for r in results if r.get('status') == 'OK') / max(ok, 1)
rh = ''
for r in results:
    sc = 'ok' if r.get('status') == 'OK' else 'fail'
    ts = r.get('total_time_ms', 0) / 1000
    qs = (r.get('query_time_ms') or 0) / 1000
    rc = r.get('row_count', '--') if r.get('row_count') is not None else '--'
    sq = (r.get('sql') or '--').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    an = (r.get('answer') or '--').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    eh = ''
    if r.get('error'):
        eh = '<div class="eb">' + str(r['error']).replace('<', '&lt;').replace('>', '&gt;') + '</div>'
    rh += '<div class="qc ' + sc + '"><div class="qh"><b>Q' + str(r.get("idx", "?")) + '</b> <span class="qs ' + sc + '">' + str(r.get("status", "?")) + '</span> ' + str(r.get("question", "")) + '</div><div class="mr"><div class="m"><span class="l">Total</span><span class="v">' + ('%.1fs' % ts) + '</span></div><div class="m"><span class="l">Query</span><span class="v">' + ('%.1fs' % qs) + '</span></div><div class="m"><span class="l">Rows</span><span class="v">' + str(rc) + '</span></div></div>' + eh + '<details><summary>SQL</summary><pre>' + sq + '</pre></details><details><summary>Answer</summary><div class="at">' + an + '</div></details></div>'
css = '*{margin:0;padding:0;box-sizing:border-box}body{font-family:sans-serif;background:#0f0f1a;color:#e0e0e0;padding:20px}h1{text-align:center;color:#a78bfa;margin-bottom:8px}.su{display:flex;justify-content:center;gap:30px;margin-bottom:24px;padding:16px;background:#1a1a2e;border-radius:12px}.su .st{text-align:center}.su .st .n{font-size:28px;font-weight:700}.su .st .lb{font-size:12px;color:#888}.n.g{color:#4ade80}.n.r{color:#f87171}.n.b{color:#60a5fa}.qc{background:#1a1a2e;border-radius:10px;padding:16px;margin-bottom:12px;border-left:4px solid #4ade80}.qc.fail{border-left-color:#f87171}.qh{margin-bottom:10px;font-size:14px}.qs{font-size:11px;padding:2px 8px;border-radius:4px;font-weight:600}.qs.ok{background:#064e3b;color:#4ade80}.qs.fail{background:#7f1d1d;color:#f87171}.mr{display:flex;gap:20px;margin-bottom:8px}.m .l{font-size:11px;color:#888;display:block}.m .v{font-size:16px;font-weight:600;color:#60a5fa}.eb{background:#7f1d1d;color:#fca5a5;padding:8px 12px;border-radius:6px;font-size:13px;margin:6px 0}details{margin-top:6px}summary{cursor:pointer;font-size:13px;color:#a78bfa;padding:4px 0}pre{background:#16213e;padding:10px;border-radius:6px;font-size:12px;overflow-x:auto;margin-top:4px;white-space:pre-wrap}.at{font-size:13px;line-height:1.6;margin-top:4px;white-space:pre-wrap}'
ts_str = time.strftime("%Y-%m-%d %H:%M UTC")
h = '<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Benchmark</title><style>' + css + '</style></head><body><h1>Forecast Engine Benchmark Report</h1><p style="text-align:center;color:#888;margin-bottom:16px">50 Queries - ' + ts_str + ' (final)</p><div class="su"><div class="st"><div class="n g">' + str(ok) + '</div><div class="lb">PASSED</div></div><div class="st"><div class="n r">' + str(fail) + '</div><div class="lb">FAILED</div></div><div class="st"><div class="n b">' + ('%.1fs' % (avg_t/1000)) + '</div><div class="lb">AVG TOTAL</div></div><div class="st"><div class="n b">' + ('%.1fs' % (avg_q/1000)) + '</div><div class="lb">AVG QUERY</div></div></div>' + rh + '</body></html>'
with open('static/benchmark_report.html', 'w') as f:
    f.write(h)
print("Report: %d/%d passed, %d failed" % (ok, len(results), fail))
for x in results:
    if x.get('status') != 'OK':
        print("  FAIL Q%d: %s" % (x['idx'], x['status']))
