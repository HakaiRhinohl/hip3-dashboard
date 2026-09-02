import { useMemo, useState } from "react";
import {
  ComposedChart, Bar, Line, Area, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const C = {
  bg: "#080b14", card: "#0e1225", border: "#1a2040", borderLight: "#252d55",
  subtle: "#3a4268", text: "#e8edf5", muted: "#5a6487",
  cyan: "#00d4ff", amber: "#ffb020", purple: "#7c5cfc", green: "#00e5a0", red: "#ff4d6a",
};

const SLICE_COLORS = [C.green, C.cyan, C.purple, C.amber, C.red, "#38bdf8", "#f472b6", "#2dd4bf"];

const fmt = (n) => {
  if (n == null || !Number.isFinite(Number(n))) return "—";
  n = Number(n);
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};

const shortAddr = (a) => (a && a.length > 12 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a || "—");

const fmtTime = (ms) => {
  if (!ms) return "—";
  const d = new Date(ms);
  return d.toISOString().slice(0, 16).replace("T", " ");
};

function StatCard({ label, value, sub, accent }) {
  return (
    <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: "14px 16px", position: "relative", overflow: "hidden" }}>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${accent}, transparent)` }} />
      <div style={{ color: C.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: "0.08em", fontWeight: 600 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 700, marginTop: 4, color: C.text }}>{value}</div>
      {sub && <div style={{ color: C.muted, fontSize: 10, marginTop: 3 }}>{sub}</div>}
    </div>
  );
}

function ConfirmBadge({ confirmed }) {
  const color = confirmed ? C.green : C.muted;
  return (
    <span style={{
      display: "inline-block", fontSize: 8, letterSpacing: "0.06em", textTransform: "uppercase",
      color, background: `${color}18`, border: `1px solid ${color}55`, borderRadius: 20, padding: "2px 7px",
    }}>
      {confirmed ? "confirmed" : "inferred"}
    </span>
  );
}

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "#131836", border: `1px solid ${C.borderLight}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, boxShadow: "0 8px 32px rgba(0,0,0,0.5)" }}>
      <p style={{ color: C.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color, margin: "2px 0", fontWeight: 500 }}>
          {p.name}: {typeof p.value === "number" ? fmt(p.value) : p.value}
        </p>
      ))}
    </div>
  );
};

function FlowTable({ rows, totalLabel }) {
  if (!rows.length) {
    return <div style={{ padding: 24, textAlign: "center", color: C.muted, fontSize: 11 }}>No {totalLabel} recorded yet</div>;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, minWidth: 640 }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${C.border}` }}>
            {["Address", "Role", "Status", "Amount", "% of total", "Tokens", "Txs"].map((h, i) => (
              <th key={i} style={{ padding: "6px 10px", textAlign: i >= 3 ? "right" : "left", color: C.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.address} style={{ borderBottom: `1px solid ${C.border}` }}>
              <td style={{ padding: "8px 10px", color: C.text, fontSize: 10, overflowWrap: "anywhere" }}>{r.address}</td>
              <td style={{ padding: "8px 10px", color: r.label ? C.text : C.muted }}>{r.label || "Unidentified"}</td>
              <td style={{ padding: "8px 10px" }}><ConfirmBadge confirmed={r.confirmed} /></td>
              <td style={{ padding: "8px 10px", textAlign: "right", color: C.text, fontWeight: 600 }}>{fmt(r.usd)}</td>
              <td style={{ padding: "8px 10px", textAlign: "right", color: C.muted }}>{r.pct}%</td>
              <td style={{ padding: "8px 10px", textAlign: "right", color: C.muted, fontSize: 9 }}>{Object.keys(r.tokens || {}).join(", ")}</td>
              <td style={{ padding: "8px 10px", textAlign: "right", color: C.muted }}>{r.tx_count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function BuybacksDashboard() {
  const { data, loading, error, refetch } = useApiData("/api/buybacks");
  const [tab, setTab] = useState("overview");

  const sourcePie = useMemo(() => {
    const sources = data?.sources || [];
    const major = sources.filter((s) => s.pct >= 3);
    const rest = sources.filter((s) => s.pct < 3);
    const restUsd = rest.reduce((s, r) => s + r.usd, 0);
    const slices = major.map((s) => ({ name: s.label || shortAddr(s.address), value: s.usd, confirmed: s.confirmed }));
    if (restUsd > 0) slices.push({ name: `Other (${rest.length} wallets)`, value: restUsd, confirmed: false });
    return slices;
  }, [data]);

  if (loading) return <Loading message="Loading buybacks data..." />;
  if (error && !data) return <ErrorState error={error} onRetry={refetch} />;
  if (!data || data.status === "loading") return <Loading message="Waiting for buybacks data..." />;

  const totals = data.totals || {};
  const sources = data.sources || [];
  const destinations = data.destinations || [];
  const dailyChart = data.daily_chart || [];
  const recent = data.recent_transactions || [];

  const tabs = [
    { id: "overview", label: "Overview" },
    { id: "sources", label: "Sources" },
    { id: "destinations", label: "Destinations" },
    { id: "timeline", label: "Timeline" },
  ];

  return (
    <div className="buybacks-page" style={{ background: C.bg, color: C.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <style>{`
        .buybacks-kpis { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); }
        @media (max-width: 1050px) {
          .buybacks-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        @media (max-width: 680px) {
          .buybacks-page { padding: 14px 12px !important; }
          .buybacks-kpis { grid-template-columns: 1fr; }
          .buybacks-tabs { overflow-x: auto; }
          .buybacks-tabs button { white-space: nowrap; padding: 8px 11px !important; }
          .buybacks-content { padding: 14px !important; }
        }
      `}</style>

      <div style={{ marginBottom: 24 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: C.amber, boxShadow: `0 0 12px ${C.amber}` }} />
          <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>sKNTQ Buybacks</h1>
        </div>
        <p style={{ color: C.muted, fontSize: 11, margin: "4px 0 0 18px" }}>
          Reconstructed from the buyback wallet's on-chain ledger · updated {data.generated_at}
        </p>
      </div>

      {/* KPIs */}
      <div className="buybacks-kpis" style={{ gap: 10, marginBottom: 20 }}>
        <StatCard label="Total Inbound" value={fmt(totals.inbound_usd)} sub={`${totals.transaction_count} ledger events`} accent={C.cyan} />
        <StatCard label="KNTQ Bought" value={fmt(totals.kntq_bought_usd)} sub={totals.kntq_bought_fill_count ? `${totals.kntq_bought_fill_count.toLocaleString("en-US")} buy fills · actual market purchases` : "Actual on-market purchases"} accent={C.amber} />
        <StatCard label="KNTQ Held in Wallet" value={fmt(totals.held_kntq_cost_basis_usd)} sub={totals.held_kntq_amount ? `${Math.round(totals.held_kntq_amount).toLocaleString("en-US")} KNTQ · bought, not yet forwarded` : "Bought, not yet forwarded"} accent={C.purple} />
        <StatCard label="Confirmed Source Coverage" value={`${totals.confirmed_inbound_pct ?? 0}%`} sub={`${fmt(totals.confirmed_inbound_usd)} matched to known wallets`} accent={C.green} />
      </div>

      {/* Tabs */}
      <div className="buybacks-tabs" style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: `1px solid ${C.border}` }}>
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: "transparent", color: tab === t.id ? C.amber : C.muted,
            border: "none", borderBottom: tab === t.id ? `2px solid ${C.amber}` : "2px solid transparent",
            padding: "8px 16px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit",
          }}>{t.label}</button>
        ))}
      </div>

      <div className="buybacks-content" style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20, minHeight: 420 }}>

        {tab === "overview" && (
          <div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
              <div>
                <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 12px", fontWeight: 600 }}>Where the money comes from</h3>
                {sourcePie.length > 0 ? (
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie data={sourcePie} cx="50%" cy="50%" innerRadius={55} outerRadius={90} dataKey="value" paddingAngle={2}
                        label={({ percent }) => `${(percent * 100).toFixed(0)}%`}
                        labelLine={{ stroke: C.muted, strokeWidth: 1 }} style={{ fontSize: 9, fontFamily: "inherit" }}>
                        {sourcePie.map((s, i) => <Cell key={i} fill={SLICE_COLORS[i % SLICE_COLORS.length]} stroke="none" opacity={s.confirmed ? 1 : 0.55} />)}
                      </Pie>
                      <Tooltip formatter={(v) => fmt(v)} />
                      <Legend wrapperStyle={{ fontSize: 9 }} />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div style={{ height: 260, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>No source data yet</div>
                )}
                <div style={{ color: C.muted, fontSize: 9, marginTop: 6, textAlign: "center" }}>Faded slices are inferred, not doc-confirmed roles</div>
              </div>
              <div>
                <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 12px", fontWeight: 600 }}>Recent transactions</h3>
                <div style={{ overflowX: "auto", maxHeight: 300, overflowY: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, minWidth: 380 }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                        {["Time", "Dir", "Counterparty", "Token", "USD"].map((h, i) => (
                          <th key={i} style={{ padding: "5px 8px", textAlign: i >= 4 ? "right" : "left", color: C.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase", position: "sticky", top: 0, background: C.card }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {recent.slice(0, 15).map((tx, i) => (
                        <tr key={tx.hash || i} style={{ borderBottom: `1px solid ${C.border}` }}>
                          <td style={{ padding: "5px 8px", color: C.muted }}>{fmtTime(tx.time)}</td>
                          <td style={{ padding: "5px 8px", color: tx.direction === "in" ? C.green : C.amber, fontWeight: 700 }}>{tx.direction === "in" ? "IN" : "OUT"}</td>
                          <td style={{ padding: "5px 8px", color: C.text }}>{shortAddr(tx.counterparty)}</td>
                          <td style={{ padding: "5px 8px", color: C.muted }}>{tx.token}</td>
                          <td style={{ padding: "5px 8px", textAlign: "right", color: C.text }}>{fmt(tx.usd)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}

        {tab === "sources" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 6px", fontWeight: 600 }}>Inbound funding sources</h3>
            <p style={{ color: C.muted, fontSize: 10, margin: "0 0 16px" }}>Every wallet that has ever sent funds into the buyback wallet, ranked by USD value</p>
            <FlowTable rows={sources} totalLabel="inbound transfers" />
          </div>
        )}

        {tab === "destinations" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 6px", fontWeight: 600 }}>KNTQ forwarding</h3>
            <p style={{ color: C.muted, fontSize: 10, margin: "0 0 16px" }}>
              Where previously-bought KNTQ is sent onward from the buyback wallet, in infrequent batches — not when the buyback itself happened. See the Timeline tab for actual daily buy activity.
            </p>
            <FlowTable rows={destinations} totalLabel="outbound transfers" />
          </div>
        )}

        {tab === "timeline" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 4px", fontWeight: 600 }}>Daily KNTQ Buybacks</h3>
            <p style={{ color: C.muted, fontSize: 10, margin: "0 0 16px" }}>Actual spot market buy fills on the KNTQ/USDC and KNTQ/USDH pairs — this is when and how much KNTQ was really bought each day</p>
            {dailyChart.length > 0 ? (
              <ResponsiveContainer width="100%" height={360}>
                <ComposedChart data={dailyChart}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                  <XAxis dataKey="date" tickFormatter={(v) => v.slice(5)} tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={6} />
                  <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                  <Area yAxisId="d" type="monotone" dataKey="inbound_usd" name="Daily Inbound" stroke="none" fill={C.cyan} fillOpacity={0.25} />
                  <Bar yAxisId="d" dataKey="kntq_bought_usd" name="Daily KNTQ Bought" fill={C.amber} opacity={0.85} isAnimationActive={false} />
                  <Line yAxisId="c" type="monotone" dataKey="cum_kntq_bought_usd" name="Cumulative Bought" stroke={C.amber} strokeWidth={2} dot={false} />
                  <Line yAxisId="c" type="monotone" dataKey="cum_inbound_usd" name="Cumulative Inbound" stroke={C.cyan} strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 360, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>No timeline data available</div>
            )}
          </div>
        )}
      </div>

      <div style={{ marginTop: 16, fontSize: 9, color: C.subtle, textAlign: "center", maxWidth: 900, marginLeft: "auto", marginRight: "auto" }}>
        {data.methodology}
      </div>
    </div>
  );
}
