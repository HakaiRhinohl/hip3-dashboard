import { useState, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const C = {
  km: "#00e5a0", xyz: "#7c5cfc", flx: "#ff4d6a", cash: "#ffb020",
  bg: "#060911", card: "#0c1020", border: "#151d38", subtle: "#1a2545",
  text: "#e4eaf3", muted: "#4f5e82",
};
const DEX_ORDER = ["km", "xyz", "flx", "cash"];

const fmt = (n) => {
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
};

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "#0f1530", border: `1px solid ${C.border}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, boxShadow: "0 8px 32px rgba(0,0,0,0.6)" }}>
      <p style={{ color: C.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color || C.text, margin: "2px 0" }}>
          {p.name}: {typeof p.value === "number" ? (p.dataKey?.includes("spread") ? p.value.toFixed(2) + " bps" : fmt(p.value)) : p.value}
        </p>
      ))}
    </div>
  );
};

const StatCard = ({ label, value, sub, highlight, rank, rankTotal }) => (
  <div style={{ background: C.subtle, borderRadius: 8, padding: "12px 14px", position: "relative" }}>
    {rank != null && (
      <div style={{
        position: "absolute", top: 8, right: 8,
        fontSize: 9, fontWeight: 700,
        color: rank === 1 ? C.km : C.muted,
        opacity: 0.8,
      }}>
        #{rank}/{rankTotal}
      </div>
    )}
    <div style={{ color: C.muted, fontSize: 9, textTransform: "uppercase", marginBottom: 4 }}>{label}</div>
    <div style={{ fontSize: 18, fontWeight: 700, color: highlight || C.text, lineHeight: 1.1 }}>{value}</div>
    {sub && <div style={{ color: C.muted, fontSize: 10, marginTop: 2 }}>{sub}</div>}
  </div>
);

export default function LiquidityDashboard() {
  const { data: apiData, loading, error, refetch } = useApiData("/api/liquidity");
  const [ticker, setTicker] = useState("SILVER");
  const [deployer, setDeployer] = useState("km");

  const tickerData = useMemo(() => {
    if (!apiData?.summary) return [];
    return DEX_ORDER
      .map((dex) => apiData.summary.find((s) => s.ticker === ticker && s.dex === dex))
      .filter(Boolean);
  }, [apiData, ticker]);

  const selectedDexData = useMemo(() => {
    return tickerData.find((d) => d.dex === deployer) || null;
  }, [tickerData, deployer]);

  const spreadRange = useMemo(() => {
    return tickerData.map((d) => ({
      name: d.name,
      median: d.spread,
      p5: d.spreadP5,
      p95: d.spreadP95,
      color: C[d.dex],
    }));
  }, [tickerData]);

  const depthByDist = useMemo(() => {
    return [
      { dist: "±10 bps", ...Object.fromEntries(tickerData.map((d) => [d.dex, d.d10])) },
      { dist: "±50 bps", ...Object.fromEntries(tickerData.map((d) => [d.dex, d.d50])) },
      { dist: "±100 bps", ...Object.fromEntries(tickerData.map((d) => [d.dex, d.d100])) },
    ];
  }, [tickerData]);

  if (loading) return <Loading message="Loading liquidity data..." />;
  if (error && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  const d = apiData;
  const TICKERS = d.tickers || ["SILVER", "GOLD", "NVDA", "TSLA"];
  const dexNames = d.dex_names || {};

  const sortedBySpread = [...tickerData].sort((a, b) => a.spread - b.spread);
  const spreadRank = selectedDexData ? sortedBySpread.findIndex((x) => x.dex === deployer) + 1 : null;
  const isSpreadBest = spreadRank === 1;
  const isSpreadWorst = spreadRank === tickerData.length;

  const spreads = tickerData.map((x) => x.spread);
  const bestSpread = spreads.length ? Math.min(...spreads) : 0;
  const worstSpread = spreads.length ? Math.max(...spreads) : 0;

  const d10s = tickerData.map((x) => x.d10);
  const d50s = tickerData.map((x) => x.d50);
  const d100s = tickerData.map((x) => x.d100);

  const rankBadgeColor = spreadRank === 1 ? C.km : spreadRank === tickerData.length ? "#ff6b6b" : C.muted;

  return (
    <div style={{ background: C.bg, color: C.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>HIP-3 Liquidity Analysis</h1>
        <p style={{ color: C.muted, fontSize: 11, margin: "4px 0 0" }}>
          Orderbook spread & depth — {d.total_snapshots} snapshots — updated {d.generated_at}
        </p>
      </div>

      {/* Shared ticker selector */}
      <div style={{ display: "flex", gap: 6, marginBottom: 16 }}>
        {TICKERS.map((t) => (
          <button key={t} onClick={() => setTicker(t)} style={{
            background: ticker === t ? "#1a2d50" : "transparent",
            color: ticker === t ? C.text : C.muted,
            border: `1px solid ${ticker === t ? "#2a4070" : C.border}`,
            borderRadius: 6, padding: "8px 18px", fontSize: 12, fontWeight: 600,
            cursor: "pointer", fontFamily: "inherit",
          }}>{t}</button>
        ))}
      </div>

      {/* Two-column layout: LEFT overview, RIGHT single-pair stats */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 16, alignItems: "start" }}>

        {/* LEFT: Comparison overview */}
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

          {/* Spread distribution */}
          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20 }}>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 14px", fontWeight: 600 }}>{ticker} Spread Distribution</h3>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {spreadRange.map((sr, i) => {
                const maxP95 = Math.max(...spreadRange.map((x) => x.p95), 1);
                const scale = 100 / maxP95;
                return (
                  <div key={i}>
                    <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, marginBottom: 3 }}>
                      <span style={{ color: sr.color, fontWeight: 600 }}>{sr.name}</span>
                      <span style={{ color: C.muted }}>median {sr.median.toFixed(2)} bps (p5: {sr.p5.toFixed(1)}, p95: {sr.p95.toFixed(1)})</span>
                    </div>
                    <div style={{ position: "relative", height: 16, background: C.subtle, borderRadius: 3 }}>
                      <div style={{ position: "absolute", top: 2, bottom: 2, borderRadius: 2, left: `${sr.p5 * scale}%`, width: `${(sr.p95 - sr.p5) * scale}%`, background: sr.color, opacity: 0.3 }} />
                      <div style={{ position: "absolute", top: 0, bottom: 0, width: 3, borderRadius: 2, left: `${sr.median * scale}%`, background: sr.color }} />
                    </div>
                  </div>
                );
              })}
            </div>
            <div style={{ marginTop: 10, fontSize: 9, color: C.muted }}>Bar = p5–p95 range · Line = median · Lower = better</div>
          </div>

          {/* Depth by distance */}
          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20 }}>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 14px", fontWeight: 600 }}>{ticker} Depth by Distance</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={depthByDist} barCategoryGap="15%">
                <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                <XAxis dataKey="dist" tick={{ fill: C.text, fontSize: 10 }} />
                <YAxis tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={(v) => fmt(v)} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10 }} />
                {DEX_ORDER.map((dx) => (
                  <Bar key={dx} dataKey={dx} name={dexNames[dx] || dx} fill={C[dx]} opacity={0.85} radius={[2, 2, 0, 0]} />
                ))}
              </BarChart>
            </ResponsiveContainer>
            <div style={{ marginTop: 8, fontSize: 9, color: C.muted }}>Deeper books absorb larger orders without slippage</div>
          </div>

          {/* Full comparison table */}
          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20, marginBottom: 16 }}>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 14px", fontWeight: 600 }}>{ticker} — Full Comparison</h3>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead><tr style={{ borderBottom: `1px solid ${C.border}` }}>
                {["Dex", "Spread (med)", "Spread (p5)", "Spread (p95)", "Depth ±10bp", "Depth ±50bp", "Depth ±100bp", "Mid Price"].map((h, i) => (
                  <th key={i} style={{ padding: "6px 8px", textAlign: i === 0 ? "left" : "right", color: C.muted, fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {tickerData.map((td) => {
                  const isBest = td.spread === Math.min(...tickerData.map((x) => x.spread));
                  const isWorst = td.spread === Math.max(...tickerData.map((x) => x.spread));
                  const isSelected = td.dex === deployer;
                  return (
                    <tr
                      key={td.dex}
                      onClick={() => setDeployer(td.dex)}
                      style={{
                        borderBottom: `1px solid ${C.subtle}`,
                        background: isSelected ? C[td.dex] + "12" : "transparent",
                        cursor: "pointer",
                      }}
                    >
                      <td style={{ padding: "6px 8px", fontWeight: 600 }}>
                        <span style={{ color: C[td.dex] }}>●</span> {td.name}
                        {isSelected && <span style={{ color: C[td.dex], fontSize: 9, marginLeft: 6 }}>◀</span>}
                      </td>
                      <td style={{ padding: "6px 8px", textAlign: "right", fontWeight: 700, color: isBest ? C.km : isWorst ? "#ff6b6b" : C.text }}>{td.spread.toFixed(2)} bps</td>
                      <td style={{ padding: "6px 8px", textAlign: "right", color: C.muted }}>{td.spreadP5.toFixed(2)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right", color: C.muted }}>{td.spreadP95.toFixed(2)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(td.d10)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(td.d50)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{fmt(td.d100)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right", color: C.muted }}>${td.mid.toFixed(2)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <div style={{ marginTop: 8, fontSize: 9, color: C.muted }}>Click a row to inspect in the stats panel →</div>
          </div>
        </div>

        {/* RIGHT: Single pair stats panel */}
        <div style={{
          background: C.card,
          border: `1px solid ${C.border}`,
          borderRadius: 10,
          padding: 20,
          position: "sticky",
          top: 64,
        }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Single Pair Stats</h3>

          {/* Deployer selector */}
          <div style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 7 }}>Deployer</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
              {DEX_ORDER.map((dx) => {
                const dxData = tickerData.find((x) => x.dex === dx);
                const isActive = deployer === dx;
                return (
                  <button key={dx} onClick={() => setDeployer(dx)} style={{
                    background: isActive ? C[dx] + "1a" : "transparent",
                    color: isActive ? C[dx] : C.muted,
                    border: `1px solid ${isActive ? C[dx] + "55" : C.border}`,
                    borderRadius: 6, padding: "7px 12px", fontSize: 11, fontWeight: 600,
                    cursor: "pointer", fontFamily: "inherit",
                    display: "flex", justifyContent: "space-between", alignItems: "center",
                    transition: "all 0.15s",
                  }}>
                    <span>{dexNames[dx] || dx}</span>
                    {dxData && (
                      <span style={{ fontSize: 10, opacity: 0.75 }}>{dxData.spread.toFixed(2)} bps</span>
                    )}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Market selector (synced with ticker tabs) */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 7 }}>Market</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 5 }}>
              {TICKERS.map((t) => (
                <button key={t} onClick={() => setTicker(t)} style={{
                  background: ticker === t ? "#1a2d50" : "transparent",
                  color: ticker === t ? C.text : C.muted,
                  border: `1px solid ${ticker === t ? "#2a4070" : C.border}`,
                  borderRadius: 6, padding: "7px 10px", fontSize: 11, fontWeight: 600,
                  cursor: "pointer", fontFamily: "inherit", textAlign: "center",
                  transition: "all 0.15s",
                }}>{t}</button>
              ))}
            </div>
          </div>

          <div style={{ height: 1, background: C.border, marginBottom: 16 }} />

          {/* Stats */}
          {selectedDexData ? (
            <div>
              {/* Spread */}
              <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Spread</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 14 }}>
                <StatCard
                  label="Median"
                  value={selectedDexData.spread.toFixed(2)}
                  sub="bps"
                  highlight={isSpreadBest ? C.km : isSpreadWorst ? "#ff6b6b" : undefined}
                  rank={spreadRank}
                  rankTotal={tickerData.length}
                />
                <StatCard label="P5" value={selectedDexData.spreadP5.toFixed(2)} sub="bps" />
                <StatCard label="P95" value={selectedDexData.spreadP95.toFixed(2)} sub="bps" />
              </div>

              {/* Spread position bar */}
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.muted, marginBottom: 4 }}>
                  <span>vs competitors</span>
                  <span>best {bestSpread.toFixed(2)} → worst {worstSpread.toFixed(2)}</span>
                </div>
                <div style={{ position: "relative", height: 6, background: C.subtle, borderRadius: 3, overflow: "visible" }}>
                  {tickerData.map((td) => {
                    const range = worstSpread - bestSpread;
                    const pct = range === 0 ? 0 : ((td.spread - bestSpread) / range) * 100;
                    return (
                      <div key={td.dex} style={{
                        position: "absolute",
                        top: 0, bottom: 0, width: 6, borderRadius: 1,
                        left: `calc(${pct.toFixed(1)}% - 3px)`,
                        background: C[td.dex],
                        opacity: td.dex === deployer ? 1 : 0.35,
                        zIndex: td.dex === deployer ? 2 : 1,
                      }} />
                    );
                  })}
                </div>
              </div>

              {/* Depth */}
              <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Depth (USD)</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 14 }}>
                <StatCard
                  label="±10 bps"
                  value={fmt(selectedDexData.d10)}
                  rank={[...d10s].sort((a, b) => b - a).indexOf(selectedDexData.d10) + 1}
                  rankTotal={tickerData.length}
                />
                <StatCard
                  label="±50 bps"
                  value={fmt(selectedDexData.d50)}
                  rank={[...d50s].sort((a, b) => b - a).indexOf(selectedDexData.d50) + 1}
                  rankTotal={tickerData.length}
                />
                <StatCard
                  label="±100 bps"
                  value={fmt(selectedDexData.d100)}
                  rank={[...d100s].sort((a, b) => b - a).indexOf(selectedDexData.d100) + 1}
                  rankTotal={tickerData.length}
                />
              </div>

              {/* Mid price */}
              <div style={{
                background: C.subtle, borderRadius: 8, padding: "10px 14px",
                display: "flex", justifyContent: "space-between", alignItems: "center",
                marginBottom: 14,
              }}>
                <div style={{ color: C.muted, fontSize: 9, textTransform: "uppercase" }}>Mid Price</div>
                <div style={{ fontSize: 16, fontWeight: 700 }}>${selectedDexData.mid.toFixed(2)}</div>
              </div>

              {/* Summary badge */}
              <div style={{
                textAlign: "center", fontSize: 10, padding: "8px 12px",
                borderRadius: 6, fontWeight: 600,
                background: isSpreadBest ? C.km + "18" : isSpreadWorst ? "#ff6b6b18" : C.subtle,
                color: rankBadgeColor,
                border: `1px solid ${rankBadgeColor}33`,
              }}>
                {isSpreadBest
                  ? `✓ Tightest spread for ${ticker}`
                  : isSpreadWorst
                  ? `↑ Widest spread for ${ticker}`
                  : `#${spreadRank} of ${tickerData.length} by spread · ${ticker}`}
              </div>
            </div>
          ) : (
            <div style={{ color: C.muted, fontSize: 11, textAlign: "center", padding: "24px 0" }}>
              No data for this pair
            </div>
          )}

          <div style={{ marginTop: 16, fontSize: 9, color: C.subtle, textAlign: "center" }}>
            {d.total_snapshots} snapshots · 30s interval
          </div>
        </div>
      </div>

      <div style={{ marginTop: 14, fontSize: 9, color: C.subtle, textAlign: "center" }}>
        L2 snapshots · 30s interval · Hyperliquid API · {d.generated_at}
      </div>
    </div>
  );
}
