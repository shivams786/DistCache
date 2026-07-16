package dashboard

import "net/http"

func Serve(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DistCache Admin</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172026; background: #f7f8fa; }
    * { box-sizing: border-box; }
    body { margin: 0; }
    header { padding: 18px 24px; border-bottom: 1px solid #dfe4ea; background: #ffffff; display: flex; justify-content: space-between; align-items: center; gap: 16px; }
    h1 { margin: 0; font-size: 21px; letter-spacing: 0; }
    main { max-width: 1180px; margin: 0 auto; padding: 24px; display: grid; gap: 20px; }
    .stats { display: grid; grid-template-columns: repeat(6, minmax(130px, 1fr)); gap: 12px; }
    .stat, .panel, table { background: #ffffff; border: 1px solid #dfe4ea; border-radius: 8px; }
    .stat { padding: 14px; min-height: 86px; }
    .label { color: #5d6975; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 25px; font-weight: 700; margin-top: 8px; }
    .panel { padding: 16px; overflow: hidden; }
    .panel h2 { margin: 0 0 12px; font-size: 16px; }
    table { width: 100%; border-collapse: collapse; overflow: hidden; }
    th, td { text-align: left; padding: 12px 14px; border-bottom: 1px solid #edf0f3; font-size: 14px; }
    th { color: #5d6975; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; background: #fbfcfd; }
    tr:last-child td { border-bottom: 0; }
    .status { display: inline-flex; align-items: center; gap: 8px; font-weight: 650; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: #d64545; }
    .healthy .dot { background: #168a4a; }
    .events { display: grid; gap: 8px; }
    .event { padding: 10px 12px; border: 1px solid #edf0f3; border-radius: 6px; background: #fbfcfd; display: grid; gap: 4px; }
    .event strong { font-size: 13px; }
    .event span { color: #5d6975; font-size: 12px; }
    @media (max-width: 860px) { .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); } header { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <header>
    <h1>DistCache Admin</h1>
    <span id="updated" class="label">Loading</span>
  </header>
  <main>
    <section class="stats">
      <div class="stat"><div class="label">Healthy Nodes</div><div id="healthyNodes" class="value">0/0</div></div>
      <div class="stat"><div class="label">Entries</div><div id="entries" class="value">0</div></div>
      <div class="stat"><div class="label">Hit Ratio</div><div id="hitRatio" class="value">No requests yet</div></div>
      <div class="stat"><div class="label">Requests</div><div id="requests" class="value">0</div></div>
      <div class="stat"><div class="label">Evictions</div><div id="evictions" class="value">0</div></div>
      <div class="stat"><div class="label">Replication / Failover</div><div id="failures" class="value">0</div></div>
    </section>
    <section class="panel">
      <h2>Cluster Nodes</h2>
      <table>
        <thead><tr><th>Node</th><th>Status</th><th>Entries</th><th>Requests</th><th>Last Successful Check</th></tr></thead>
        <tbody id="nodes"></tbody>
      </table>
    </section>
    <section class="panel">
      <h2>Recent Events</h2>
      <div id="events" class="events"></div>
    </section>
  </main>
  <script>
    const fmt = new Intl.NumberFormat();
    async function json(url) {
      const response = await fetch(url, {cache: "no-store"});
      if (!response.ok) throw new Error(url + " " + response.status);
      return response.json();
    }
    function pct(hits, misses) {
      const total = hits + misses;
      return total === 0 ? "No requests yet" : Math.round((hits / total) * 100) + "%";
    }
    async function refresh() {
      const [cluster, stats, events] = await Promise.all([
        json("/admin/api/cluster"),
        json("/admin/api/stats"),
        json("/admin/api/events")
      ]);
      const healthy = cluster.nodes.filter(n => n.healthy).length;
      healthyNodes.textContent = healthy + "/" + cluster.nodes.length;
      entries.textContent = fmt.format(stats.cache.entries);
      hitRatio.textContent = pct(stats.cache.hits, stats.cache.misses);
      requests.textContent = fmt.format(stats.requests_total);
      evictions.textContent = fmt.format(stats.cache.evictions);
      failures.textContent = fmt.format(stats.metrics.replication_failures + stats.metrics.failovers);
      nodes.innerHTML = cluster.nodes.map(n => {
        const check = n.last_successful_check ? new Date(n.last_successful_check).toLocaleTimeString() : "-";
        return "<tr><td>" + n.node.id + "</td><td><span class='status " + (n.healthy ? "healthy" : "") + "'><span class='dot'></span>" + (n.healthy ? "healthy" : "unhealthy") + "</span></td><td>" + fmt.format(n.cache_entries) + "</td><td>" + fmt.format(n.request_count) + "</td><td>" + check + "</td></tr>";
      }).join("");
      document.getElementById("events").innerHTML = events.events.map(e => "<div class='event'><strong>" + e.type + " - " + e.level + "</strong><span>" + new Date(e.time).toLocaleTimeString() + " - " + e.message + "</span></div>").join("") || "<div class='event'><span>No events yet</span></div>";
      updated.textContent = "Updated " + new Date().toLocaleTimeString();
    }
    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 3000);
  </script>
</body>
</html>`
