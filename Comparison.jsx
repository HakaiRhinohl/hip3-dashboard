import { useState, useMemo } from "react";
import {
  BarChart, Bar, AreaChart, Area, ComposedChart,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_COLORS = { km: "#00e5a0", xyz: "#7c5cfc", flx: "#ff4d6a", cash: "#ffb020" };
const DEX_SHORT = { km: "KM", xyz: "XYZ", flx: "FLX", cash: "CASH" };
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

export default function ComparisonDashboard() {
  const { data: apiData, loading, error, refetch } = useApiData("/api/comparison");
  const [tab, setTab] = useState("overview");

  // Find the earliest date where km has data (for "since Markets launch" filter)
  const overlap = useMemo(() => {
    if (!apiData?.daily_chart) return [];
    return apiData.daily_chart.filter((d) => d.km_vol > 0 || d.km_cum > 0);
  }, [apiData]);

  if (loading) return <Loading message="Loading comparison data..." />;
  if (error && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  const d = apiData;
  const dexes = d.dex_summaries || [];
  const chart = d.daily_chart || [];
  const kmBps = d.km_bps || { growth: 0.4074, normal: 4.0743 };

  const tabs = [
    { id: "overview", l: "Overview" },
    { id: "volume", l: "Volume" },
    { id: "implied", l: "Implied KM Revenue" },
    { id: "tickers", l: "Tickers" },
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
              <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "'IBM Plex Sans'", color: DEX_COLORS[dx.dex] }}>{dx.name}</span>
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
        {tab === "overview" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Cumulative Volume</h3>
            <ResponsiveContainer width="100%" height={320}>
              <AreaChart data={chart}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="date" tick={{ fill: P.muted, fontSize: 8 }} tickLine={false} interval={10} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Area type="monotone" dataKey="xyz_cum" name="Trade.xyz" stroke={DEX_COLORS.xyz} fill={DEX_COLORS.xyz + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="cash_cum" name="Dreamcash" stroke={DEX_COLORS.cash} fill={DEX_COLORS.cash + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="flx_cum" name="Felix" stroke={DEX_COLORS.flx} fill={DEX_COLORS.flx + "20"} strokeWidth={2} />
                <Area type="monotone" dataKey="km_cum" name="Markets" stroke={DEX_COLORS.km} fill={DEX_COLORS.km + "20"} strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
            {/* Summary table */}
            <div style={{ marginTop: 16, overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                <thead><tr style={{ borderBottom: `1px solid ${P.border}` }}>
                  {["", "Tickers", "Cum Volume", "Deployer Fees", "Builder", "Net Deposit", "30d Avg/d"].map((h, i) => (
                    <th key={i} style={{ padding: "6px 8px", textAlign: i === 0 ? "left" : "right", color: P.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>{dexes.map((dx) => (
                  <tr key={dx.dex} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "6px 8px", fontWeight: 600 }}><span style={{ color: DEX_COLORS[dx.dex] }}>●</span> {dx.name}</td>
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

        {tab === "volume" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Daily Volume (since Markets launch)</h3>
            <ResponsiveContainer width="100%" height={340}>
              <ComposedChart data={overlap}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="date" tick={{ fill: P.muted, fontSize: 9 }} tickLine={false} interval={3} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Bar dataKey="xyz_vol" name="Trade.xyz" fill={DEX_COLORS.xyz} opacity={0.5} />
                <Bar dataKey="cash_vol" name="Dreamcash" fill={DEX_COLORS.cash} opacity={0.5} />
                <Bar dataKey="flx_vol" name="Felix" fill={DEX_COLORS.flx} opacity={0.6} />
                <Bar dataKey="km_vol" name="Markets" fill={DEX_COLORS.km} opacity={0.8} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {tab === "implied" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 4px", fontWeight: 600 }}>Implied Kinetiq Revenue</h3>
            <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
              If Markets had each dex's volume at {kmBps.growth.toFixed(4)} bps (growth) / {kmBps.normal.toFixed(4)} bps (normal)
            </p>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={dexes.map((dx) => ({
                name: DEX_SHORT[dx.dex] || dx.dex,
                growth: dx.implied_km_growth_ann,
                normal: dx.implied_km_normal_ann,
              })).sort((a, b) => b.normal - a.normal)} barCategoryGap="20%">
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
                <XAxis dataKey="name" tick={{ fill: P.text, fontSize: 11 }} />
                <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} />
                <Tooltip formatter={(v) => fmt(v)} /><Legend wrapperStyle={{ fontSize: 10 }} />
                <Bar dataKey="growth" name="Growth Mode" fill={DEX_COLORS.km} opacity={0.9} radius={[3, 3, 0, 0]} />
                <Bar dataKey="normal" name="Normal Mode" fill="#6366f1" opacity={0.7} radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
            {/* Table */}
            <div style={{ marginTop: 14, overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                <thead><tr style={{ borderBottom: `1px solid ${P.border}` }}>
                  {["Scenario", "30d Vol/day", "KM Growth/yr", "KM Normal/yr", "KM Normal/mo"].map((h, i) => (
                    <th key={i} style={{ padding: "6px 8px", textAlign: i === 0 ? "left" : "right", color: P.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>{dexes.map((dx) => (
                  <tr key={dx.dex} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "6px 8px", fontWeight: 600 }}><span style={{ color: DEX_COLORS[dx.dex] }}>●</span> {dx.name}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.avg_30d)}/d</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.implied_km_growth_ann)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600 }}>{fmt(dx.implied_km_normal_ann)}</td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(dx.implied_km_normal_ann / 12)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </div>
        )}

        {tab === "tickers" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Top Tickers by Volume</h3>
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${dexes.length}, 1fr)`, gap: 14 }}>
              {dexes.map((dx) => {
                const tks = dx.top_tickers || [];
                const mx = tks[0]?.volume || 1;
                return (
                  <div key={dx.dex}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: DEX_COLORS[dx.dex], marginBottom: 6 }}>{dx.name}</div>
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
