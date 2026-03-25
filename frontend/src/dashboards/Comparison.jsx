import { useState, useMemo } from "react";
import {
  BarChart, Bar, AreaChart, Area, ComposedChart, Cell,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_COLORS = { km: "#00e5a0", xyz: "#7c5cfc", flx: "#ff4d6a", cash: "#ffb020" };
const DEX_NAMES  = { km: "Markets", xyz: "Trade.xyz", flx: "Felix", cash: "Dreamcash" };
const P = { bg: "#060911", card: "#0c1020", border: "#151d38", subtle: "#1a2545", text: "#e4eaf3", muted: "#4f5e82" };

const fmt = (n) => {
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};
const fS = (n) => {
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(0)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(0)}K`;
  return n.toFixed(0);
};

const pctColor = (v) => v === null ? P.muted : v > 0 ? "#00e5a0" : "#ff4d6a";
const pctFmt  = (v) => v === null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(1)}%`;

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "#0f1530", border: `1px solid ${P.border}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, boxShadow: "0 8px 32px rgba(0,0,0,0.6)" }}>
      <p style={{ color: P.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.filter((p) => p.value > 0).map((p, i) => (
        <p key={i} style={{ color: p.color, margin: "2px 0" }}>{p.name}: {fmt(p.value)}</p>
      ))}
    </div>
  );
};

const ShareTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "#0f1530", border: `1px solid ${P.border}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, boxShadow: "0 8px 32px rgba(0,0,0,0.6)" }}>
      <p style={{ color: P.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {[...payload].reverse().filter((p) => p.value > 0.05).map((p, i) => (
        <p key={i} style={{ color: p.color, margin: "2px 0" }}>{p.name}: {p.value.toFixed(1)}%</p>
      ))}
    </div>
  );
};

const DEXES = ["km", "xyz", "flx", "cash"];

export default function ComparisonDashboard() {
  const { data: apiData, loading, error, refetch } = useApiData("/api/comparison");
  const [tab, setTab] = useState("overview");

  // All data since Trade.xyz launch
  const overlap = useMemo(() => {
    if (!apiData?.daily_chart) return [];
    return apiData.daily_chart.filter((d) => d.xyz_vol > 0 || d.xyz_cum > 0);
  }, [apiData]);

  // 7d / 30d volume trends per DEX
  const trends = useMemo(() => {
    const days = apiData?.daily_chart || [];
    const vol = (slice, key) => slice.reduce((s, d) => s + (d[key] || 0), 0);
    return DEXES.map((dex) => {
      const key    = `${dex}_vol`;
      const last7  = vol(days.slice(-7),    key);
      const prev7  = vol(days.slice(-14, -7), key);
      const last30 = vol(days.slice(-30),   key);
      const prev30 = vol(days.slice(-60, -30), key);
      return {
        dex,
        vol7d:  last7,
        vol30d: last30,
        pct7d:  prev7  > 0 ? (last7  - prev7)  / prev7  * 100 : null,
        pct30d: prev30 > 0 ? (last30 - prev30) / prev30 * 100 : null,
      };
    });
  }, [apiData]);

  // Market share over time — normalized to %
  const shareData = useMemo(() => {
    return overlap.map((d) => {
      const total = DEXES.reduce((s, dx) => s + (d[`${dx}_vol`] || 0), 0);
      if (!total) return null;
      const row = { date: d.date };
      DEXES.forEach((dx) => { row[dx] = (d[`${dx}_vol`] || 0) / total * 100; });
      return row;
    }).filter(Boolean);
  }, [overlap]);

  if (loading) return <Loading message="Loading comparison data..." />;
  if (error && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  const d = apiData;
  const dexes = d.dex_summaries || [];
  const chart  = d.daily_chart  || [];

  const sortedTrends = [...trends].sort((a, b) => b.vol7d - a.vol7d);

  const tabs = [
    { id: "overview", l: "Overview"     },
    { id: "volume",   l: "Volume"       },
    { id: "trends",   l: "Trends"       },
    { id: "share",    l: "Market Share" },
    { id: "tickers",  l: "Tickers"      },
  ];

  return (
    <div style={{ background: P.bg, color: P.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>HIP-3 Market Comparison</h1>
        <p style={{ color: P.muted, fontSize: 11, margin: "4px 0 0" }}>Markets vs Trade.xyz vs Felix vs Dreamcash — updated {d.generated_at}</p>
      </div>

      {/* Summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 20 }}>
        {dexes.map((dx) => (
          <div key={dx.dex} style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 8, padding: "14px 16px", position: "relative", overflow: "hidden" }}>
            <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${DEX_COLORS[dx.dex]}, transparent)` }} />
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "'IBM Plex Sans'", color: DEX_COLORS[dx.dex] }}>{DEX_NAMES[dx.dex] || dx.name}</span>
              <span style={{ fontSize: 10, color: P.muted }}>{dx.num_tickers}t · {dx.num_days}d</span>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
              <div><div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase" }}>Volume</div><div style={{ fontSize: 15, fontWeight: 700 }}>{fmt(dx.cum_volume)}</div></div>
              <div><div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase" }}>Fees</div><div style={{ fontSize: 15, fontWeight: 700 }}>{fmt(dx.total_fees)}</div></div>
              <div><div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase" }}>Net Deposit</div><div style={{ fontSize: 13, fontWeight: 600 }}>{fmt(dx.total_net_deposit)}</div></div>
              <div><div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase" }}>30d Avg</div><div style={{ fontSize: 13, fontWeight: 600 }}>{fmt(dx.avg_30d)}/d</div></div>
            </div>
          </div>
        ))}
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: `1px solid ${P.border}` }}>
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: "transparent", color: tab === t.id ? DEX_COLORS.km : P.muted,
            border: "none", borderBottom: tab === t.id ? `2px solid ${DEX_COLORS.km}` : "2px solid transparent",
            padding: "8px 14px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit",
          }}>{t.l}</button>
        ))}
      </div>

      <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20, minHeight: 440 }}>

        {/* ── Overview ── */}
        {tab === "overview" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Cumulative Volume</h3>
            <ResponsiveContainer width="100%" height={320}>
              <AreaChart data={chart}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="date" tick={{ fill: P.muted, fontSize: 8 }} tickLine={false} interval={10} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Area type="monotone" dataKey="xyz_cum"  name="Trade.xyz" stroke={DEX_COLORS.xyz}  fill={DEX_COLORS.xyz  + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="cash_cum" name="Dreamcash" stroke={DEX_COLORS.cash} fill={DEX_COLORS.cash + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="flx_cum"  name="Felix"     stroke={DEX_COLORS.flx}  fill={DEX_COLORS.flx  + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="km_cum"   name="Markets"   stroke={DEX_COLORS.km}   fill={DEX_COLORS.km   + "20"} strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
            <div style={{ marginTop: 16, overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                <thead><tr style={{ borderBottom: `1px solid ${P.border}` }}>
                  {["", "Tickers", "Cum Volume", "Deployer Fees", "Builder", "Net Deposit", "30d Avg/d"].map((h, i) => (
                    <th key={i} style={{ padding: "6px 8px", textAlign: i === 0 ? "left" : "right", color: P.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>{dexes.map((dx) => (
                  <tr key={dx.dex} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "6px 8px", fontWeight: 600 }}><span style={{ color: DEX_COLORS[dx.dex] }}>●</span> {DEX_NAMES[dx.dex] || dx.name}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{dx.num_tickers}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600 }}>{fmt(dx.cum_volume)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.deployer_fees)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.builder_fees)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.total_net_deposit)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.avg_30d)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </div>
        )}

        {/* ── Volume ── */}
        {tab === "volume" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Daily Volume (since Trade.xyz launch)</h3>
            <ResponsiveContainer width="100%" height={360}>
              <ComposedChart data={overlap}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="date" tick={{ fill: P.muted, fontSize: 9 }} tickLine={false} interval={3} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Bar dataKey="xyz_vol"  name="Trade.xyz" fill={DEX_COLORS.xyz}  opacity={0.6} stackId="a" barSize={5} />
                <Bar dataKey="cash_vol" name="Dreamcash" fill={DEX_COLORS.cash} opacity={0.6} stackId="a" barSize={5} />
                <Bar dataKey="flx_vol"  name="Felix"     fill={DEX_COLORS.flx}  opacity={0.7} stackId="a" barSize={5} />
                <Bar dataKey="km_vol"   name="Markets"   fill={DEX_COLORS.km}   opacity={0.9} stackId="a" barSize={5} radius={[2, 2, 0, 0]} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* ── Trends ── */}
        {tab === "trends" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 4px", fontWeight: 600 }}>Volume Trends</h3>
            <p style={{ color: P.muted, fontSize: 10, margin: "0 0 20px" }}>Week-over-week and month-over-month volume change per DEX</p>

            {/* Trend table */}
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, marginBottom: 28 }}>
              <thead><tr style={{ borderBottom: `1px solid ${P.border}` }}>
                {["DEX", "Last 7d", "vs prev 7d", "Last 30d", "vs prev 30d"].map((h, i) => (
                  <th key={i} style={{ padding: "8px 12px", textAlign: i === 0 ? "left" : "right", color: P.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {sortedTrends.map(({ dex, vol7d, vol30d, pct7d, pct30d }) => (
                  <tr key={dex} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "10px 12px", fontWeight: 600 }}>
                      <span style={{ color: DEX_COLORS[dex] }}>●</span>{" "}{DEX_NAMES[dex]}
                    </td>
                    <td style={{ padding: "10px 12px", textAlign: "right", fontWeight: 600 }}>{fmt(vol7d)}</td>
                    <td style={{ padding: "10px 12px", textAlign: "right" }}>
                      <span style={{ color: pctColor(pct7d), fontWeight: 700 }}>{pctFmt(pct7d)}</span>
                    </td>
                    <td style={{ padding: "10px 12px", textAlign: "right", fontWeight: 600 }}>{fmt(vol30d)}</td>
                    <td style={{ padding: "10px 12px", textAlign: "right" }}>
                      <span style={{ color: pctColor(pct30d), fontWeight: 700 }}>{pctFmt(pct30d)}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* 7d vs 30d bar chart */}
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 12px", fontWeight: 600 }}>7d vs 30d Volume</h3>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={sortedTrends.map((t) => ({ name: DEX_NAMES[t.dex], "7d": t.vol7d, "30d": t.vol30d, _dex: t.dex }))} barCategoryGap="25%">
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} vertical={false} />
                <XAxis dataKey="name" tick={{ fill: P.text, fontSize: 10 }} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} tickLine={false} axisLine={false} />
                <Tooltip formatter={(v) => fmt(v)} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                <Bar dataKey="7d" name="Last 7d" radius={[3, 3, 0, 0]}>
                  {sortedTrends.map((t) => <Cell key={t.dex} fill={DEX_COLORS[t.dex]} />)}
                </Bar>
                <Bar dataKey="30d" name="Last 30d" radius={[3, 3, 0, 0]} opacity={0.4}>
                  {sortedTrends.map((t) => <Cell key={t.dex} fill={DEX_COLORS[t.dex]} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* ── Market Share ── */}
        {tab === "share" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 4px", fontWeight: 600 }}>Market Share over Time</h3>
            <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>Daily volume share (%) since Trade.xyz launch</p>
            <ResponsiveContainer width="100%" height={380}>
              <AreaChart data={shareData}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="date" tick={{ fill: P.muted, fontSize: 8 }} tickLine={false} interval={5} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={(v) => `${v.toFixed(0)}%`} tickLine={false} axisLine={false} domain={[0, 100]} />
                <Tooltip content={<ShareTip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Area type="monotone" dataKey="xyz"  name="Trade.xyz" stroke={DEX_COLORS.xyz}  fill={DEX_COLORS.xyz}  fillOpacity={0.75} stackId="1" />
                <Area type="monotone" dataKey="cash" name="Dreamcash" stroke={DEX_COLORS.cash} fill={DEX_COLORS.cash} fillOpacity={0.75} stackId="1" />
                <Area type="monotone" dataKey="flx"  name="Felix"     stroke={DEX_COLORS.flx}  fill={DEX_COLORS.flx}  fillOpacity={0.75} stackId="1" />
                <Area type="monotone" dataKey="km"   name="Markets"   stroke={DEX_COLORS.km}   fill={DEX_COLORS.km}   fillOpacity={0.85} stackId="1" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* ── Tickers ── */}
        {tab === "tickers" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Top Tickers by Volume</h3>
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${dexes.length}, 1fr)`, gap: 14 }}>
              {dexes.map((dx) => {
                const tks = dx.top_tickers || [];
                const mx  = tks[0]?.volume || 1;
                return (
                  <div key={dx.dex}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: DEX_COLORS[dx.dex], marginBottom: 6 }}>{DEX_NAMES[dx.dex] || dx.name}</div>
                    {tks.map((t, i) => (
                      <div key={i} style={{ marginBottom: 3 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, marginBottom: 1 }}>
                          <span style={{ color: P.text }}>{t.ticker}</span>
                          <span style={{ color: P.muted }}>{fmt(t.volume)} ({t.pct}%)</span>
                        </div>
                        <div style={{ height: 3, background: P.subtle, borderRadius: 2 }}>
                          <div style={{ height: 3, borderRadius: 2, background: DEX_COLORS[dx.dex], opacity: 0.7, width: `${(t.volume / mx * 100).toFixed(1)}%` }} />
                        </div>
                      </div>
                    ))}
                  </div>
                );
              })}
            </div>
          </div>
        )}

      </div>

      <div style={{ marginTop: 14, fontSize: 9, color: P.subtle, textAlign: "center" }}>
        Hyperliquid L1 API · Auto-refresh every 5 min · {d.generated_at}
      </div>
    </div>
  );
}
