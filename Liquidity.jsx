import { useState, useMemo, useEffect, useRef } from "react";
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

// ── Constants ──────────────────────────────────────────────────────────────────

const C = {
  km: "#00e5a0", xyz: "#7c5cfc", flx: "#ff4d6a", cash: "#ffb020",
  bg: "#060911", card: "#0c1020", border: "#151d38", subtle: "#1a2545",
  text: "#e4eaf3", muted: "#4f5e82",
};
const DEX_ORDER   = ["km", "xyz", "flx", "cash"];
const HOUR_OPTIONS = [1, 2, 3, 4, 12, 24];

// ── Formatters ─────────────────────────────────────────────────────────────────

const fmt = (n) => {
  if (n == null) return "—";
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
};

const fmtTs = (ts) => {
  const d = new Date(ts * 1000);
  return `${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}`;
};

// ── Tooltip ────────────────────────────────────────────────────────────────────

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background:"#0f1530", border:`1px solid ${C.border}`, borderRadius:6, padding:"8px 12px", fontSize:11, boxShadow:"0 8px 32px rgba(0,0,0,.6)" }}>
      <p style={{ color:C.muted, marginBottom:4, fontWeight:600, fontSize:10 }}>{label}</p>
      {payload.filter(p => p.value != null).map((p, i) => (
        <p key={i} style={{ color:p.color||C.text, margin:"2px 0" }}>
          {p.name}: {typeof p.value === "number"
            ? String(p.name).toLowerCase().includes("spread") ? `${p.value.toFixed(2)} bps` : fmt(p.value)
            : p.value}
        </p>
      ))}
    </div>
  );
};

// ── StatCard ───────────────────────────────────────────────────────────────────

const StatCard = ({ label, value, sub, highlight, rank, rankTotal }) => (
  <div style={{ background:C.subtle, borderRadius:8, padding:"12px 14px", position:"relative" }}>
    {rank != null && (
      <div style={{ position:"absolute", top:8, right:8, fontSize:9, fontWeight:700, color:rank===1?C.km:C.muted, opacity:.8 }}>
        #{rank}/{rankTotal}
      </div>
    )}
    <div style={{ color:C.muted, fontSize:9, textTransform:"uppercase", marginBottom:4 }}>{label}</div>
    <div style={{ fontSize:18, fontWeight:700, color:highlight||C.text, lineHeight:1.1 }}>{value}</div>
    {sub && <div style={{ color:C.muted, fontSize:10, marginTop:2 }}>{sub}</div>}
  </div>
);

// ── Ticker search combobox ─────────────────────────────────────────────────────

function TickerSearch({ allTickers, tickersByDex, value, onChange }) {
  const [open,  setOpen]  = useState(false);
  const [query, setQuery] = useState("");
  const ref = useRef(null);

  useEffect(() => {
    const fn = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, []);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    return allTickers.filter(t => t.toLowerCase().includes(q));
  }, [allTickers, query]);

  return (
    <div ref={ref} style={{ position:"relative", minWidth:200 }}>
      <div
        onClick={() => { setOpen(o => !o); setQuery(""); }}
        style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:"7px 14px", fontSize:12, fontWeight:600, color:C.text, cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"space-between", userSelect:"none", gap:8 }}
      >
        <span>{value || "Select ticker…"}</span>
        <span style={{ color:C.muted, fontSize:10 }}>▼</span>
      </div>

      {open && (
        <div style={{ position:"absolute", top:"calc(100% + 4px)", left:0, right:0, zIndex:200, background:"#0d1428", border:`1px solid ${C.border}`, borderRadius:8, boxShadow:"0 8px 32px rgba(0,0,0,.7)", overflow:"hidden" }}>
          <div style={{ padding:"8px 10px", borderBottom:`1px solid ${C.border}` }}>
            <input
              autoFocus value={query} onChange={e => setQuery(e.target.value)}
              placeholder="Search…"
              style={{ width:"100%", background:"transparent", border:"none", outline:"none", color:C.text, fontSize:12, fontFamily:"inherit" }}
            />
          </div>
          <div style={{ maxHeight:260, overflowY:"auto" }}>
            {filtered.length === 0
              ? <div style={{ padding:"10px 14px", color:C.muted, fontSize:11 }}>No results</div>
              : filtered.map(t => (
                <div
                  key={t}
                  onClick={() => { onChange(t); setOpen(false); setQuery(""); }}
                  style={{ padding:"7px 14px", fontSize:12, fontWeight:t===value?700:400, color:t===value?C.text:C.muted, background:t===value?C.subtle:"transparent", cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"space-between" }}
                  onMouseEnter={e => e.currentTarget.style.background = C.subtle}
                  onMouseLeave={e => e.currentTarget.style.background = t===value ? C.subtle : "transparent"}
                >
                  <span>{t}</span>
                  {/* DEX coverage dots */}
                  <span style={{ display:"flex", gap:3 }}>
                    {DEX_ORDER.map(dex => {
                      const has = (tickersByDex[dex] || []).includes(t);
                      return <span key={dex} style={{ width:5, height:5, borderRadius:"50%", background:has?C[dex]:C.border, opacity:has?1:.3 }} />;
                    })}
                  </span>
                </div>
              ))
            }
          </div>
        </div>
      )}
    </div>
  );
}

// ── Timeseries chart ───────────────────────────────────────────────────────────

function buildChartData(series, field) {
  if (!series) return [];
  const allTs = new Set();
  Object.values(series).forEach(s => s.forEach(p => allTs.add(p.t)));
  const sorted = Array.from(allTs).sort((a, b) => a - b);
  return sorted.map(t => {
    const pt = { t, label: fmtTs(t) };
    Object.entries(series).forEach(([dex, pts]) => {
      const snap = pts.find(p => p.t === t);
      pt[dex] = snap?.[field] ?? null;
    });
    return pt;
  });
}

function TimeseriesChart({ title, series, field, yFormatter, dexNames, dexesPresent }) {
  const data = useMemo(() => buildChartData(series, field), [series, field]);
  if (!data.length) return null;
  return (
    <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:10, padding:20 }}>
      <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:13, margin:"0 0 14px", fontWeight:600 }}>{title}</h3>
      <ResponsiveContainer width="100%" height={170}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={.3} />
          <XAxis dataKey="label" tick={{ fill:C.muted, fontSize:9 }} tickLine={false} interval="preserveStartEnd" />
          <YAxis tick={{ fill:C.muted, fontSize:9 }} tickFormatter={yFormatter} tickLine={false} axisLine={false} width={52} />
          <Tooltip content={<Tip />} />
          <Legend wrapperStyle={{ fontSize:10 }} />
          {dexesPresent.map(dex => (
            <Line key={dex} type="monotone" dataKey={dex} name={dexNames[dex]||dex}
              stroke={C[dex]} strokeWidth={1.5} dot={false} connectNulls />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Main dashboard ─────────────────────────────────────────────────────────────

export default function LiquidityDashboard() {
  const [ticker,   setTicker]   = useState("SILVER");
  const [deployer, setDeployer] = useState("km");
  const [hours,    setHours]    = useState(4);

  const { data: apiData, loading, error, refetch } =
    useApiData(`/api/liquidity?hours=${hours}`);

  const { data: tsData } =
    useApiData(`/api/liquidity/timeseries?ticker=${encodeURIComponent(ticker)}&hours=${hours}`);

  if (loading && !apiData) return <Loading message="Loading liquidity data…" />;
  if (error   && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  const d          = apiData;
  const allTickers = d?.tickers        || [];
  const tkByDex    = d?.tickers_by_dex || {};
  const dexNames   = d?.dex_names      || {};

  const dexesWithTicker = DEX_ORDER.filter(dex => (tkByDex[dex] || []).includes(ticker));

  const tickerData = useMemo(() => {
    if (!d?.summary) return [];
    return DEX_ORDER
      .map(dex => d.summary.find(s => s.ticker === ticker && s.dex === dex))
      .filter(Boolean);
  }, [d, ticker]);

  const spreadRange = useMemo(() => tickerData.map(s => ({
    name:s.name, median:s.spread, p5:s.spreadP5, p95:s.spreadP95, color:C[s.dex],
  })), [tickerData]);

  const depthByDist = useMemo(() => [
    { dist:"±10 bps",  ...Object.fromEntries(tickerData.map(s => [s.dex, s.d10])) },
    { dist:"±50 bps",  ...Object.fromEntries(tickerData.map(s => [s.dex, s.d50])) },
    { dist:"±100 bps", ...Object.fromEntries(tickerData.map(s => [s.dex, s.d100])) },
  ], [tickerData]);

  // Right panel
  const selData      = tickerData.find(s => s.dex === deployer) || null;
  const sortedBySpr  = [...tickerData].sort((a,b) => a.spread - b.spread);
  const spreadRank   = selData ? sortedBySpr.findIndex(x => x.dex === deployer) + 1 : null;
  const isBestSpr    = spreadRank === 1;
  const isWorstSpr   = spreadRank === tickerData.length && tickerData.length > 1;
  const spreads      = tickerData.map(x => x.spread);
  const bestSpr      = spreads.length ? Math.min(...spreads) : 0;
  const worstSpr     = spreads.length ? Math.max(...spreads) : 0;
  const d10s  = tickerData.map(x => x.d10);
  const d50s  = tickerData.map(x => x.d50);
  const d100s = tickerData.map(x => x.d100);
  const badgeColor = isBestSpr ? C.km : isWorstSpr ? "#ff6b6b" : C.muted;

  const tsDexes = tsData?.series ? Object.keys(tsData.series) : [];

  return (
    <div style={{ background:C.bg, color:C.text, minHeight:"100vh", fontFamily:"'IBM Plex Mono', monospace", padding:"20px 24px" }}>

      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <h1 style={{ fontFamily:"'IBM Plex Sans'", fontSize:22, fontWeight:700, margin:0 }}>HIP-3 Liquidity Analysis</h1>
        <p style={{ color:C.muted, fontSize:11, margin:"4px 0 0" }}>
          {d?.pairs_monitored} pairs monitored · {d?.total_snapshots} snapshots · {d?.generated_at}
        </p>
      </div>

      {/* Controls: time range + ticker search */}
      <div style={{ display:"flex", alignItems:"center", gap:12, marginBottom:16, flexWrap:"wrap" }}>

        {/* Time range buttons */}
        <div style={{ display:"flex", gap:4 }}>
          {HOUR_OPTIONS.map(h => (
            <button key={h} onClick={() => setHours(h)} style={{
              background: hours===h ? "#1a2d50" : "transparent",
              color:      hours===h ? C.text : C.muted,
              border:    `1px solid ${hours===h ? "#2a4070" : C.border}`,
              borderRadius:6, padding:"6px 14px", fontSize:11, fontWeight:600,
              cursor:"pointer", fontFamily:"inherit",
            }}>
              {h}h
            </button>
          ))}
        </div>

        <div style={{ width:1, height:28, background:C.border }} />

        {/* Ticker combobox */}
        <TickerSearch
          allTickers={allTickers}
          tickersByDex={tkByDex}
          value={ticker}
          onChange={t => {
            setTicker(t);
            // Keep deployer only if it has this ticker
            const newDexes = DEX_ORDER.filter(dex => (tkByDex[dex] || []).includes(t));
            if (!newDexes.includes(deployer)) setDeployer(newDexes[0] || "km");
          }}
        />

        {/* DEX coverage pills */}
        <div style={{ display:"flex", gap:6, alignItems:"center" }}>
          {DEX_ORDER.map(dex => {
            const has = (tkByDex[dex] || []).includes(ticker);
            return (
              <span key={dex} style={{ fontSize:10, color:has?C[dex]:C.muted, fontWeight:has?700:400, opacity:has?1:.35 }}>
                {dexNames[dex] || dex}
              </span>
            );
          })}
        </div>
      </div>

      {/* Two-column layout */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 320px", gap:16, alignItems:"start" }}>

        {/* LEFT */}
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

          {/* Spread distribution */}
          <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:10, padding:20 }}>
            <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:14, margin:"0 0 14px", fontWeight:600 }}>
              {ticker} — Spread Distribution&nbsp;<span style={{ color:C.muted, fontWeight:400 }}>({hours}h)</span>
            </h3>
            {spreadRange.length === 0
              ? <div style={{ color:C.muted, fontSize:11 }}>No data for this ticker in the selected window.</div>
              : <div style={{ display:"flex", flexDirection:"column", gap:12 }}>
                  {spreadRange.map((sr, i) => {
                    const mx = Math.max(...spreadRange.map(x => x.p95), 1);
                    const sc = 100 / mx;
                    return (
                      <div key={i}>
                        <div style={{ display:"flex", justifyContent:"space-between", fontSize:10, marginBottom:3 }}>
                          <span style={{ color:sr.color, fontWeight:600 }}>{sr.name}</span>
                          <span style={{ color:C.muted }}>median {sr.median.toFixed(2)} bps &nbsp;(p5: {sr.p5.toFixed(1)}, p95: {sr.p95.toFixed(1)})</span>
                        </div>
                        <div style={{ position:"relative", height:16, background:C.subtle, borderRadius:3 }}>
                          <div style={{ position:"absolute", top:2, bottom:2, borderRadius:2, left:`${sr.p5*sc}%`, width:`${(sr.p95-sr.p5)*sc}%`, background:sr.color, opacity:.3 }} />
                          <div style={{ position:"absolute", top:0, bottom:0, width:3, borderRadius:2, left:`${sr.median*sc}%`, background:sr.color }} />
                        </div>
                      </div>
                    );
                  })}
                </div>
            }
            <div style={{ marginTop:10, fontSize:9, color:C.muted }}>Bar = p5–p95 · Line = median · Lower = better</div>
          </div>

          {/* Depth by distance */}
          {tickerData.length > 0 && (
            <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:10, padding:20 }}>
              <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:14, margin:"0 0 14px", fontWeight:600 }}>
                {ticker} — Depth by Distance&nbsp;<span style={{ color:C.muted, fontWeight:400 }}>({hours}h)</span>
              </h3>
              <ResponsiveContainer width="100%" height={180}>
                <BarChart data={depthByDist} barCategoryGap="15%">
                  <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={.3} />
                  <XAxis dataKey="dist" tick={{ fill:C.text, fontSize:10 }} />
                  <YAxis tick={{ fill:C.muted, fontSize:9 }} tickFormatter={v => fmt(v)} />
                  <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize:10 }} />
                  {dexesWithTicker.map(dx => (
                    <Bar key={dx} dataKey={dx} name={dexNames[dx]||dx} fill={C[dx]} opacity={.85} radius={[2,2,0,0]} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
              <div style={{ marginTop:8, fontSize:9, color:C.muted }}>Deeper books absorb larger orders without slippage</div>
            </div>
          )}

          {/* Full comparison table */}
          {tickerData.length > 0 && (
            <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:10, padding:20, marginBottom:0 }}>
              <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:14, margin:"0 0 14px", fontWeight:600 }}>
                {ticker} — Full Comparison&nbsp;<span style={{ color:C.muted, fontWeight:400 }}>({hours}h)</span>
              </h3>
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
                <thead><tr style={{ borderBottom:`1px solid ${C.border}` }}>
                  {["Dex","Spread (med)","Spread (p5)","Spread (p95)","Depth ±10bp","Depth ±50bp","Depth ±100bp","Mid Price"].map((h,i) => (
                    <th key={i} style={{ padding:"6px 8px", textAlign:i===0?"left":"right", color:C.muted, fontWeight:600, fontSize:9, textTransform:"uppercase" }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {tickerData.map(td => {
                    const isBest  = td.spread === Math.min(...spreads);
                    const isWorst = td.spread === Math.max(...spreads) && spreads.length > 1;
                    const isSel   = td.dex === deployer;
                    return (
                      <tr key={td.dex} onClick={() => setDeployer(td.dex)} style={{ borderBottom:`1px solid ${C.subtle}`, background:isSel?C[td.dex]+"12":"transparent", cursor:"pointer" }}>
                        <td style={{ padding:"6px 8px", fontWeight:600 }}>
                          <span style={{ color:C[td.dex] }}>●</span> {td.name}
                          {isSel && <span style={{ color:C[td.dex], fontSize:9, marginLeft:6 }}>◀</span>}
                        </td>
                        <td style={{ padding:"6px 8px", textAlign:"right", fontWeight:700, color:isBest?C.km:isWorst?"#ff6b6b":C.text }}>{td.spread.toFixed(2)} bps</td>
                        <td style={{ padding:"6px 8px", textAlign:"right", color:C.muted }}>{td.spreadP5.toFixed(2)}</td>
                        <td style={{ padding:"6px 8px", textAlign:"right", color:C.muted }}>{td.spreadP95.toFixed(2)}</td>
                        <td style={{ padding:"6px 8px", textAlign:"right" }}>{fmt(td.d10)}</td>
                        <td style={{ padding:"6px 8px", textAlign:"right" }}>{fmt(td.d50)}</td>
                        <td style={{ padding:"6px 8px", textAlign:"right" }}>{fmt(td.d100)}</td>
                        <td style={{ padding:"6px 8px", textAlign:"right", color:C.muted }}>${td.mid.toFixed(2)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div style={{ marginTop:8, fontSize:9, color:C.muted }}>Click a row to inspect in the stats panel →</div>
            </div>
          )}
        </div>

        {/* RIGHT: single pair stats panel */}
        <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:10, padding:20, position:"sticky", top:64 }}>
          <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:14, margin:"0 0 16px", fontWeight:600 }}>Single Pair Stats</h3>

          {/* Deployer selector */}
          <div style={{ marginBottom:14 }}>
            <div style={{ fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:1, marginBottom:7 }}>Deployer</div>
            <div style={{ display:"flex", flexDirection:"column", gap:5 }}>
              {DEX_ORDER.map(dx => {
                const dxRow   = tickerData.find(x => x.dex === dx);
                const hasPair = (tkByDex[dx] || []).includes(ticker);
                const isAct   = deployer === dx;
                return (
                  <button key={dx} onClick={() => hasPair && setDeployer(dx)} style={{
                    background: isAct ? C[dx]+"1a" : "transparent",
                    color:      isAct ? C[dx] : hasPair ? C.muted : C.border,
                    border:    `1px solid ${isAct ? C[dx]+"55" : C.border}`,
                    borderRadius:6, padding:"7px 12px", fontSize:11, fontWeight:600,
                    cursor: hasPair ? "pointer" : "default", fontFamily:"inherit",
                    display:"flex", justifyContent:"space-between", alignItems:"center",
                    opacity: hasPair ? 1 : .35,
                  }}>
                    <span>{dexNames[dx] || dx}</span>
                    <span style={{ fontSize:10, opacity:.75 }}>
                      {dxRow ? `${dxRow.spread.toFixed(2)} bps` : "—"}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>

          <div style={{ height:1, background:C.border, marginBottom:16 }} />

          {/* Stats or empty state */}
          {selData ? (
            <div>
              <div style={{ fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:1, marginBottom:8 }}>Spread</div>
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:8, marginBottom:14 }}>
                <StatCard label="Median" value={selData.spread.toFixed(2)} sub="bps"
                  highlight={isBestSpr?C.km:isWorstSpr?"#ff6b6b":undefined}
                  rank={spreadRank} rankTotal={tickerData.length} />
                <StatCard label="P5"  value={selData.spreadP5.toFixed(2)}  sub="bps" />
                <StatCard label="P95" value={selData.spreadP95.toFixed(2)} sub="bps" />
              </div>

              {/* Relative position bar */}
              {tickerData.length > 1 && (
                <div style={{ marginBottom:14 }}>
                  <div style={{ display:"flex", justifyContent:"space-between", fontSize:9, color:C.muted, marginBottom:4 }}>
                    <span>vs competitors</span>
                    <span>best {bestSpr.toFixed(2)} → worst {worstSpr.toFixed(2)}</span>
                  </div>
                  <div style={{ position:"relative", height:6, background:C.subtle, borderRadius:3 }}>
                    {tickerData.map(td => {
                      const range = worstSpr - bestSpr;
                      const pct   = range===0 ? 0 : ((td.spread - bestSpr) / range) * 100;
                      return (
                        <div key={td.dex} style={{ position:"absolute", top:0, bottom:0, width:6, borderRadius:1, left:`calc(${pct.toFixed(1)}% - 3px)`, background:C[td.dex], opacity:td.dex===deployer?1:.35, zIndex:td.dex===deployer?2:1 }} />
                      );
                    })}
                  </div>
                </div>
              )}

              <div style={{ fontSize:9, color:C.muted, textTransform:"uppercase", letterSpacing:1, marginBottom:8 }}>Depth (USD)</div>
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:8, marginBottom:14 }}>
                <StatCard label="±10 bps"  value={fmt(selData.d10)}
                  rank={d10s.length>1?[...d10s].sort((a,b)=>b-a).indexOf(selData.d10)+1:null} rankTotal={tickerData.length} />
                <StatCard label="±50 bps"  value={fmt(selData.d50)}
                  rank={d50s.length>1?[...d50s].sort((a,b)=>b-a).indexOf(selData.d50)+1:null} rankTotal={tickerData.length} />
                <StatCard label="±100 bps" value={fmt(selData.d100)}
                  rank={d100s.length>1?[...d100s].sort((a,b)=>b-a).indexOf(selData.d100)+1:null} rankTotal={tickerData.length} />
              </div>

              <div style={{ background:C.subtle, borderRadius:8, padding:"10px 14px", display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:14 }}>
                <div style={{ color:C.muted, fontSize:9, textTransform:"uppercase" }}>Mid Price</div>
                <div style={{ fontSize:16, fontWeight:700 }}>${selData.mid.toFixed(2)}</div>
              </div>

              {tickerData.length > 1 && (
                <div style={{ textAlign:"center", fontSize:10, padding:"8px 12px", borderRadius:6, fontWeight:600, background:isBestSpr?C.km+"18":isWorstSpr?"#ff6b6b18":C.subtle, color:badgeColor, border:`1px solid ${badgeColor}33` }}>
                  {isBestSpr ? `✓ Tightest spread for ${ticker}` : isWorstSpr ? `↑ Widest spread for ${ticker}` : `#${spreadRank} of ${tickerData.length} by spread · ${ticker}`}
                </div>
              )}
            </div>
          ) : (
            <div style={{ color:C.muted, fontSize:11, textAlign:"center", padding:"24px 0" }}>
              {(tkByDex[deployer]||[]).includes(ticker)
                ? "No data yet — snapshots arriving…"
                : `${dexNames[deployer]||deployer} doesn't list ${ticker}`}
            </div>
          )}

          <div style={{ marginTop:16, fontSize:9, color:C.subtle, textAlign:"center" }}>
            {d?.total_snapshots} snapshots · 30s interval
          </div>
        </div>
      </div>

      {/* Timeseries charts */}
      {tsData?.series && tsDexes.length > 0 && (
        <div style={{ marginTop:20 }}>
          <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:12 }}>
            <h3 style={{ fontFamily:"'IBM Plex Sans'", fontSize:14, fontWeight:600, margin:0 }}>{ticker} — Over Time</h3>
            <span style={{ fontSize:10, color:C.muted }}>last {hours}h · {tsData.bucket_minutes}min buckets</span>
          </div>
          <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12 }}>
            <TimeseriesChart title="Spread (bps)"        series={tsData.series} field="spread" yFormatter={v=>`${v} bps`} dexNames={dexNames} dexesPresent={tsDexes} />
            <TimeseriesChart title="Depth ±10 bps (USD)" series={tsData.series} field="d10"    yFormatter={fmt}           dexNames={dexNames} dexesPresent={tsDexes} />
            <TimeseriesChart title="Depth ±50 bps (USD)" series={tsData.series} field="d50"    yFormatter={fmt}           dexNames={dexNames} dexesPresent={tsDexes} />
            <TimeseriesChart title="Depth ±100 bps (USD)"series={tsData.series} field="d100"   yFormatter={fmt}           dexNames={dexNames} dexesPresent={tsDexes} />
          </div>
        </div>
      )}

      <div style={{ marginTop:20, fontSize:9, color:C.subtle, textAlign:"center" }}>
        L2 snapshots · 30s interval · Hyperliquid API · {d?.generated_at}
      </div>
    </div>
  );
}
