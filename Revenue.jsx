import { useState, useMemo } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, ComposedChart, Line,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const fmt = (n) => {
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};

const C = {
  bg: "#080b14", card: "#0e1225", border: "#1a2040", borderLight: "#252d55",
  green: "#00e5a0", purple: "#7c5cfc", cyan: "#00d4ff",
  amber: "#ffb020", red: "#ff4d6a", subtle: "#3a4268",
  text: "#e8edf5", muted: "#5a6487",
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
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${accent || C.green}, transparent)` }} />
      <div style={{ color: C.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: "0.08em", fontWeight: 600 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 700, marginTop: 4, color: C.text }}>{value}</div>
      {sub && <div style={{ color: C.muted, fontSize: 10, marginTop: 3 }}>{sub}</div>}
    </div>
  );
}

export default function RevenueDashboard() {
  const { data: apiData, loading, error, refetch } = useApiData("/api/revenue");
  const [tab, setTab] = useState("revenue");

  const chartData = useMemo(() => {
    if (!apiData?.daily_chart) return [];
    let cg = 0, cn = 0, cb = 0, ctg = 0, ctn = 0;
    return apiData.daily_chart.map((d) => {
      cg += d.deployer_fee_growth;
      cn += d.deployer_fee_normal;
      cb += d.builder_fee || 0;
      ctg += d.deployer_fee_growth + (d.builder_fee || 0);
      ctn += d.deployer_fee_normal + (d.builder_fee || 0);
      return { ...d, dt: d.date.slice(5), cum_g: cg, cum_n: cn, cum_b: cb, cum_total_g: ctg, cum_total_n: ctn };
    });
  }, [apiData]);

  if (loading) return <Loading message="Loading revenue data..." />;
  if (error && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  const d = apiData;
  const proj30 = d.projections?.last_30d;
  const avg7d = d.averages?.avg_7d || 0;

  const pieData = [
    { name: "HIP-3 Deployer", value: d.fees.deployer, color: C.amber },
    { name: "Builder (Trading)", value: d.fees.trading_builder, color: C.cyan },
    { name: "Builder (Staking)", value: d.fees.staking_builder, color: C.purple },
  ];

  const tabs = [
    { id: "revenue", label: "Revenue" },
    { id: "volume", label: "Volume" },
    { id: "breakdown", label: "Breakdown" },
    { id: "tickers", label: "Tickers" },
  ];

  return (
    <div style={{ background: C.bg, color: C.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <div style={{ marginBottom: 24, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 8, height: 8, borderRadius: "50%", background: C.green, boxShadow: `0 0 12px ${C.green}` }} />
            <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>Markets by Kinetiq</h1>
          </div>
          <p style={{ color: C.muted, fontSize: 11, margin: "4px 0 0 18px" }}>HIP-3 Revenue Analysis — 100% on-chain — updated {d.generated_at}</p>
        </div>
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, padding: "6px 12px", fontSize: 10, color: C.muted }}>
          {d.km_tickers} tickers · {d.days_since_launch} days
        </div>
      </div>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 10, marginBottom: 20 }}>
        <StatCard label="Cumulative Volume" value={fmt(d.total_volume)} sub={`${fmt(avg7d)}/day (7d avg)`} accent={C.cyan} />
        <StatCard label="Total Fees" value={fmt(d.fees.total)} sub={`${fmt(d.fees.deployer)} deployer + ${fmt(d.fees.trading_builder + d.fees.staking_builder)} builder`} accent={C.amber} />
        <StatCard label="Effective Rate" value={`${d.rates.eff_deployer_bps_growth.toFixed(2)} bps`} sub={`${d.rates.eff_deployer_bps_normal.toFixed(2)} bps normal mode`} accent={C.green} />
        <StatCard label="Ann. Revenue (Growth)" value={proj30 ? fmt(proj30.growth_mode.total) : "—"} sub={proj30 ? `${fmt(proj30.growth_mode.total / 12)}/mo` : ""} accent={C.green} />
        <StatCard label="Ann. Revenue (Normal)" value={proj30 ? fmt(proj30.normal_mode.total) : "—"} sub={proj30 ? `${fmt(proj30.normal_mode.total / 12)}/mo` : ""} accent={C.purple} />
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: `1px solid ${C.border}` }}>
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: "transparent", color: tab === t.id ? C.green : C.muted,
            border: "none", borderBottom: tab === t.id ? `2px solid ${C.green}` : "2px solid transparent",
            padding: "8px 16px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit",
          }}>{t.label}</button>
        ))}
      </div>

      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20, minHeight: 420 }}>
        {tab === "revenue" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: 0, fontWeight: 600 }}>Daily Total Revenue — Growth vs Normal Mode</h3>
              <div style={{ fontSize: 10, color: C.muted }}>Bars = daily · Lines = cumulative</div>
            </div>
            <ResponsiveContainer width="100%" height={340}>
              <ComposedChart data={chartData} barGap={1}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                <XAxis dataKey="dt" tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={3} />
                <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                <Bar yAxisId="d" dataKey="deployer_fee_growth" name="Deployer (Growth)" fill={C.green} stackId="growth" opacity={0.85} />
                <Bar yAxisId="d" dataKey="builder_fee" name="Builder Fee" fill={C.cyan} stackId="growth" opacity={0.85} radius={[2, 2, 0, 0]} />
                <Bar yAxisId="d" dataKey="deployer_fee_normal" name="Deployer (Normal)" fill={C.purple} opacity={0.2} radius={[2, 2, 0, 0]} />
                <Line yAxisId="c" type="monotone" dataKey="cum_total_g" name="Cum. (Growth)" stroke={C.green} strokeWidth={2} dot={false} />
                <Line yAxisId="c" type="monotone" dataKey="cum_total_n" name="Cum. (Normal)" stroke={C.purple} strokeWidth={2} strokeDasharray="4 4" dot={false} />
              </ComposedChart>
            </ResponsiveContainer>
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
                <Bar yAxisId="d" dataKey="daily_volume_usd" name="Daily Volume" fill={C.cyan} opacity={0.6} radius={[2, 2, 0, 0]} />
                <Line yAxisId="c" type="monotone" dataKey="cum_volume_usd" name="Cumulative" stroke={C.amber} strokeWidth={2} dot={false} />
                <ReferenceLine yAxisId="d" y={avg7d} stroke={C.green} strokeDasharray="6 3" strokeWidth={1} label={{ value: "7d avg", fill: C.green, fontSize: 9, position: "right" }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {tab === "breakdown" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
            <div>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Fee Sources (Historical)</h3>
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
                Total: {fmt(d.fees.total)} · HIP-3 deployer = {((d.fees.deployer / d.fees.total) * 100).toFixed(1)}%
              </div>
            </div>
            <div>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Annualized Revenue (30d run-rate)</h3>
              {proj30 && (
                <>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={[
                      { name: "Growth", deployer: proj30.growth_mode.deployer, builder: proj30.growth_mode.builder },
                      { name: "Normal", deployer: proj30.normal_mode.deployer, builder: proj30.normal_mode.builder },
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
                    Growth: <strong style={{ color: C.green }}>{fmt(proj30.growth_mode.total)}/yr</strong>
                    {" → Normal: "}<strong style={{ color: C.purple }}>{fmt(proj30.normal_mode.total)}/yr</strong>
                    {" = "}<strong style={{ color: C.amber }}>{(proj30.normal_mode.total / proj30.growth_mode.total).toFixed(1)}x</strong>
                  </div>
                </>
              )}
            </div>
          </div>
        )}

        {tab === "tickers" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Volume by Ticker</h3>
            <ResponsiveContainer width="100%" height={380}>
              <BarChart data={d.ticker_chart} layout="vertical" margin={{ left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} horizontal={false} />
                <XAxis type="number" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} />
                <YAxis type="category" dataKey="ticker" tick={{ fill: C.text, fontSize: 10 }} width={80} />
                <Tooltip formatter={(v) => fmt(v)} />
                <Bar dataKey="volume" fill={C.cyan} radius={[0, 4, 4, 0]} opacity={0.8}
                  label={{ position: "right", fill: C.muted, fontSize: 9, formatter: (v) => `${(v / d.total_volume * 100).toFixed(1)}%` }} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      <div style={{ marginTop: 16, fontSize: 10, color: C.subtle, textAlign: "center" }}>
        Hyperliquid L1 API · Auto-refresh every 5 min · {d.generated_at}
      </div>
    </div>
  );
}
