import { useState, useMemo } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, ComposedChart, Line,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_META = {
  km: { name: "Markets by Kinetiq", short: "Kinetiq", color: "#00e5a0" },
  xyz: { name: "Trade.xyz", short: "Trade.xyz", color: "#7c5cfc" },
  flx: { name: "Felix", short: "Felix", color: "#ff4d6a" },
  cash: { name: "Dreamcash", short: "Dreamcash", color: "#ffb020" },
};

const fmt = (n) => {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};

const C = {
  bg: "#080b14", card: "#0e1225", border: "#1a2040", borderLight: "#252d55",
  subtle: "#3a4268", text: "#e8edf5", muted: "#5a6487",
  cyan: "#00d4ff", amber: "#ffb020", purple: "#7c5cfc",
};

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

export default function RevenueDashboard({ dexId = "km" }) {
  // Use comparison endpoint which has data for all dexes
  const { data: compData, loading: compLoading, error: compError, refetch: compRefetch } = useApiData("/api/comparison");
  // Also use revenue endpoint for km-specific detailed data
  const { data: revData, loading: revLoading, error: revError, refetch: revRefetch } = useApiData("/api/revenue");

  const [tab, setTab] = useState("revenue");

  const meta = DEX_META[dexId] || DEX_META.km;
  const accent = meta.color;

  // Extract this dex's data from comparison endpoint
  const dexData = useMemo(() => {
    if (!compData?.dex_summaries) return null;
    return compData.dex_summaries.find((d) => d.dex === dexId);
  }, [compData, dexId]);

  // Build daily chart for this specific dex
  const chartData = useMemo(() => {
    if (!compData?.daily_chart) return [];
    return compData.daily_chart
      .filter((d) => d[`${dexId}_vol`] > 0)
      .map((d) => {
        const vol = d[`${dexId}_vol`];
        const cum = d[`${dexId}_cum`];
        return { date: d.date, dt: d.date.slice(5), daily_volume_usd: vol, cum_volume_usd: cum };
      });
  }, [compData, dexId]);

  // For km, use the detailed revenue data; for others, derive from comparison
  const isKm = dexId === "km";

  // Compute fee chart data
  const feeChartData = useMemo(() => {
    if (isKm && revData?.daily_chart) {
      let cg = 0, cn = 0, ctg = 0, ctn = 0;
      return revData.daily_chart.map((d) => {
        cg += d.deployer_fee_growth;
        cn += d.deployer_fee_normal;
        ctg += d.deployer_fee_growth + (d.builder_fee || 0);
        ctn += d.deployer_fee_normal + (d.builder_fee || 0);
        return { ...d, dt: d.date.slice(5), cum_total_g: ctg, cum_total_n: ctn };
      });
    }
    if (!dexData || !chartData.length) return [];
    const effBps = dexData.eff_deployer_bps || 0;
    const normalBps = effBps > 0 ? effBps / 0.10 : 0;
    let cumG = 0, cumN = 0;
    return chartData.map((d) => {
      const feeG = d.daily_volume_usd * effBps / 10000;
      const feeN = d.daily_volume_usd * normalBps / 10000;
      cumG += feeG;
      cumN += feeN;
      return {
        ...d,
        deployer_fee_growth: Math.round(feeG * 100) / 100,
        deployer_fee_normal: Math.round(feeN * 100) / 100,
        builder_fee: 0,
        cum_total_g: cumG,
        cum_total_n: cumN,
      };
    });
  }, [isKm, revData, dexData, chartData]);

  const loading = compLoading || (isKm && revLoading);
  const error = compError || (isKm && revError);

  if (loading) return <Loading message={`Loading ${meta.short} data...`} />;
  if (error && !dexData) return <ErrorState error={error} onRetry={compRefetch} />;
  if (!dexData) return <Loading message={`Waiting for ${meta.short} data...`} />;

  const d = dexData;
  const fees = { deployer: d.deployer_fees || 0, builder: d.builder_fees || 0, total: d.total_fees || 0 };
  const effBps = d.eff_deployer_bps || 0;
  const normalBps = effBps > 0 ? effBps / 0.10 : 0;
  const avg7d = d.avg_7d || 0;
  const avg30d = d.avg_30d || 0;

  // Projections
  const annGrowth = avg30d * 365 * effBps / 10000;
  const annNormal = avg30d * 365 * normalBps / 10000;

  const pieData = [
    { name: "Deployer Fees", value: fees.deployer, color: C.amber },
    { name: "Builder Fees", value: fees.builder, color: C.purple },
  ].filter((p) => p.value > 0);

  const tickerData = d.top_tickers || [];

  const tabs = [
    { id: "revenue", label: "Revenue" },
    { id: "volume", label: "Volume" },
    { id: "breakdown", label: "Breakdown" },
    { id: "tickers", label: "Tickers" },
  ];

  return (
    <div style={{ background: C.bg, color: C.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      {/* Header */}
      <div style={{ marginBottom: 24, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 8, height: 8, borderRadius: "50%", background: accent, boxShadow: `0 0 12px ${accent}` }} />
            <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>{meta.name}</h1>
          </div>
          <p style={{ color: C.muted, fontSize: 11, margin: "4px 0 0 18px" }}>
            Revenue Analysis — 100% on-chain — updated {compData?.generated_at || "..."}
          </p>
        </div>
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, padding: "6px 12px", fontSize: 10, color: C.muted }}>
          {d.num_tickers} tickers · {d.num_days} days
        </div>
      </div>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 10, marginBottom: 20 }}>
        <StatCard label="Cumulative Volume" value={fmt(d.cum_volume)} sub={`${fmt(avg7d)}/day (7d avg)`} accent={C.cyan} />
        <StatCard label="Total Fees" value={fmt(fees.total)} sub={fees.builder > 0 ? `${fmt(fees.deployer)} deployer + ${fmt(fees.builder)} builder` : `${fmt(fees.deployer)} deployer`} accent={C.amber} />
        <StatCard label="Effective Rate" value={effBps > 0 ? `${effBps.toFixed(2)} bps` : "—"} sub={normalBps > 0 ? `${normalBps.toFixed(2)} bps normal mode` : "No data"} accent={accent} />
        <StatCard label="Ann. Revenue (Growth)" value={annGrowth > 0 ? fmt(annGrowth) : "—"} sub={annGrowth > 0 ? `${fmt(annGrowth / 12)}/mo` : ""} accent={accent} />
        <StatCard label="Ann. Revenue (Normal)" value={annNormal > 0 ? fmt(annNormal) : "—"} sub={annNormal > 0 ? `${fmt(annNormal / 12)}/mo` : ""} accent={C.purple} />
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: `1px solid ${C.border}` }}>
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: "transparent", color: tab === t.id ? accent : C.muted,
            border: "none", borderBottom: tab === t.id ? `2px solid ${accent}` : "2px solid transparent",
            padding: "8px 16px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit",
          }}>{t.label}</button>
        ))}
      </div>

      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20, minHeight: 420 }}>
        {tab === "revenue" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: 0, fontWeight: 600 }}>Daily Revenue — Growth vs Normal Mode</h3>
              <div style={{ fontSize: 10, color: C.muted }}>Bars = daily · Lines = cumulative</div>
            </div>
            {feeChartData.length > 0 ? (
              <ResponsiveContainer width="100%" height={340}>
                <ComposedChart data={feeChartData} barGap={1}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                  <XAxis dataKey="dt" tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={3} />
                  <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                  <Bar yAxisId="d" dataKey="deployer_fee_growth" name="Deployer (Growth)" fill={accent} stackId="g" opacity={0.85} />
                  {isKm && <Bar yAxisId="d" dataKey="builder_fee" name="Builder Fee" fill={C.cyan} stackId="g" opacity={0.85} radius={[2, 2, 0, 0]} />}
                  <Bar yAxisId="d" dataKey="deployer_fee_normal" name="Deployer (Normal)" fill={C.purple} opacity={0.2} radius={[2, 2, 0, 0]} />
                  <Line yAxisId="c" type="monotone" dataKey="cum_total_g" name="Cum. (Growth)" stroke={accent} strokeWidth={2} dot={false} />
                  <Line yAxisId="c" type="monotone" dataKey="cum_total_n" name="Cum. (Normal)" stroke={C.purple} strokeWidth={2} strokeDasharray="4 4" dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 340, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>
                No fee data available for {meta.short}
              </div>
            )}
          </div>
        )}

        {tab === "volume" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Daily Trading Volume</h3>
            <ResponsiveContainer width="100%" height={340}>
              <ComposedChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                <XAxis dataKey="dt" tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={3} />
                <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                <Bar yAxisId="d" dataKey="daily_volume_usd" name="Daily Volume" fill={accent} opacity={0.6} radius={[2, 2, 0, 0]} />
                <Line yAxisId="c" type="monotone" dataKey="cum_volume_usd" name="Cumulative" stroke={C.amber} strokeWidth={2} dot={false} />
                <ReferenceLine yAxisId="d" y={avg7d} stroke={accent} strokeDasharray="6 3" strokeWidth={1} label={{ value: "7d avg", fill: accent, fontSize: 9, position: "right" }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {tab === "breakdown" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
            <div>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Fee Sources</h3>
              {pieData.length > 0 ? (
                <>
                  <ResponsiveContainer width="100%" height={260}>
                    <PieChart>
                      <Pie data={pieData} cx="50%" cy="50%" innerRadius={55} outerRadius={90} dataKey="value" paddingAngle={2}
                        label={({ name, percent }) => `${name.split(" ")[0]} ${(percent * 100).toFixed(1)}%`}
                        labelLine={{ stroke: C.muted, strokeWidth: 1 }} style={{ fontSize: 9, fontFamily: "inherit" }}>
                        {pieData.map((dd, i) => <Cell key={i} fill={dd.color} stroke="none" />)}
                      </Pie>
                      <Tooltip formatter={(v) => fmt(v)} />
                    </PieChart>
                  </ResponsiveContainer>
                  <div style={{ textAlign: "center", color: C.muted, fontSize: 10, marginTop: 8 }}>
                    Total: {fmt(fees.total)}
                  </div>
                </>
              ) : (
                <div style={{ height: 260, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>
                  No fee breakdown available
                </div>
              )}
            </div>
            <div>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Annualized Revenue (30d run-rate)</h3>
              {annGrowth > 0 ? (
                <>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={[
                      { name: "Growth", deployer: annGrowth, builder: isKm ? (fees.builder / d.num_days * 365) : 0 },
                      { name: "Normal", deployer: annNormal, builder: isKm ? (fees.builder / d.num_days * 365) : 0 },
                    ]} layout="vertical" barCategoryGap="25%">
                      <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} horizontal={false} />
                      <XAxis type="number" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} />
                      <YAxis type="category" dataKey="name" tick={{ fill: C.text, fontSize: 11, fontWeight: 500 }} width={70} />
                      <Tooltip formatter={(v) => fmt(v)} /><Legend wrapperStyle={{ fontSize: 10 }} />
                      <Bar dataKey="deployer" name="Deployer" fill={C.amber} stackId="a" />
                      <Bar dataKey="builder" name="Builder" fill={C.purple} stackId="a" radius={[0, 3, 3, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div style={{ marginTop: 16, padding: "10px 14px", background: C.bg, borderRadius: 6, fontSize: 11, color: C.muted, textAlign: "center" }}>
                    Growth: <strong style={{ color: accent }}>{fmt(annGrowth)}/yr</strong>
                    {" → Normal: "}<strong style={{ color: C.purple }}>{fmt(annNormal)}/yr</strong>
                    {annGrowth > 0 && <> = <strong style={{ color: C.amber }}>{(annNormal / annGrowth).toFixed(1)}x</strong></>}
                  </div>
                </>
              ) : (
                <div style={{ height: 200, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>
                  Insufficient data for projections
                </div>
              )}
            </div>
          </div>
        )}

        {tab === "tickers" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Volume by Ticker</h3>
            {tickerData.length > 0 ? (
              <ResponsiveContainer width="100%" height={380}>
                <BarChart data={tickerData} layout="vertical" margin={{ left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} horizontal={false} />
                  <XAxis type="number" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} />
                  <YAxis type="category" dataKey="ticker" tick={{ fill: C.text, fontSize: 10 }} width={100} />
                  <Tooltip formatter={(v) => fmt(v)} />
                  <Bar dataKey="volume" fill={accent} radius={[0, 4, 4, 0]} opacity={0.8}
                    label={{ position: "right", fill: C.muted, fontSize: 9, formatter: (v) => `${((v / d.cum_volume) * 100).toFixed(1)}%` }} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 380, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>
                No ticker data available
              </div>
            )}
          </div>
        )}
      </div>

      <div style={{ marginTop: 16, fontSize: 10, color: C.subtle, textAlign: "center" }}>
        Hyperliquid L1 API · Auto-refresh every 5 min · {compData?.generated_at}
      </div>
    </div>
  );
}
