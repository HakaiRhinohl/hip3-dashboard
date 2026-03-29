import { useState, useMemo } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

// ── Constants ──────────────────────────────────────────────────────────────────

const DEX_COLORS = {
  km:   "#00e5a0",
  xyz:  "#7c5cfc",
  flx:  "#ff4d6a",
  cash: "#ffb020",
  hyna: "#38bdf8",
  vntl: "#f472b6",
};

const DEX_NAMES = {
  km:   "Markets",
  xyz:  "Trade.xyz",
  flx:  "Felix",
  cash: "Dreamcash",
  hyna: "Hyna",
  vntl: "Vantil",
};

const DEXES = ["km", "xyz", "flx", "cash", "hyna", "vntl"];

const PERIODS = ["7d", "30d", "90d"];

const P = {
  bg:     "#060911",
  card:   "#0a0e1a",
  border: "#141c34",
  subtle: "#1a2545",
  text:   "#e4eaf3",
  muted:  "#4a5578",
};

// ── Formatters ─────────────────────────────────────────────────────────────────

const fmtN = (n) => (n == null ? "—" : Number(n).toLocaleString());

// ── Tooltip ────────────────────────────────────────────────────────────────────

const BarTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  const total = payload.reduce((s, p) => s + (p.value || 0), 0);
  return (
    <div style={{
      background: "#0f1530", border: `1px solid ${P.border}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 11,
      boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
    }}>
      <p style={{ color: P.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.filter((p) => p.value > 0).map((p, i) => (
        <p key={i} style={{ color: p.fill, margin: "2px 0" }}>
          {DEX_NAMES[p.dataKey] || p.dataKey}: {p.value.toLocaleString()}
        </p>
      ))}
      {total > 0 && (
        <p style={{ color: P.text, marginTop: 4, borderTop: `1px solid ${P.border}`, paddingTop: 4, fontWeight: 700 }}>
          Total: {total.toLocaleString()}
        </p>
      )}
    </div>
  );
};

const DonutTip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const p = payload[0];
  return (
    <div style={{
      background: "#0f1530", border: `1px solid ${P.border}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 11,
      boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
    }}>
      <p style={{ color: p.payload.fill, fontWeight: 700, margin: 0 }}>
        {DEX_NAMES[p.name] || p.name}
      </p>
      <p style={{ color: P.text, margin: "2px 0" }}>{p.value.toLocaleString()} users</p>
      <p style={{ color: P.muted, margin: 0 }}>{p.payload.pct}%</p>
    </div>
  );
};

// ── Sub-components ─────────────────────────────────────────────────────────────

function StatCard({ title, value, subtitle, accentColor, large }) {
  return (
    <div style={{
      background: P.card, border: `1px solid ${P.border}`,
      borderRadius: 8, padding: "14px 16px",
      position: "relative", overflow: "hidden",
    }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 2,
        background: `linear-gradient(90deg, ${accentColor}, transparent)`,
      }} />
      <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 6, letterSpacing: "0.06em" }}>
        {title}
      </div>
      <div style={{ fontSize: large ? 28 : 22, fontWeight: 700, color: accentColor, lineHeight: 1.1, fontFamily: "'IBM Plex Mono', monospace" }}>
        {value}
      </div>
      {subtitle && (
        <div style={{ color: P.muted, fontSize: 10, marginTop: 4 }}>{subtitle}</div>
      )}
    </div>
  );
}

function BootstrapBanner({ status }) {
  if (!status || status.complete) return null;
  const pct = status.total_dates > 0
    ? Math.round((status.processed_dates / status.total_dates) * 100)
    : 0;
  return (
    <div style={{
      background: "#0d1a2e", border: `1px solid #1e3a5f`,
      borderRadius: 8, padding: "12px 16px", marginBottom: 16,
      display: "flex", alignItems: "center", gap: 16,
    }}>
      <div style={{ flex: 1 }}>
        <div style={{ color: "#38bdf8", fontSize: 11, fontWeight: 600, marginBottom: 6 }}>
          Bootstrap in progress — loading historical data
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ flex: 1, height: 4, background: P.subtle, borderRadius: 2, overflow: "hidden" }}>
            <div style={{
              height: 4, borderRadius: 2,
              background: "linear-gradient(90deg, #38bdf8, #7c5cfc)",
              width: `${pct}%`,
              transition: "width 0.5s ease",
            }} />
          </div>
          <span style={{ color: "#38bdf8", fontSize: 10, fontWeight: 700, whiteSpace: "nowrap" }}>
            {pct}% ({status.processed_dates.toLocaleString()} / {status.total_dates.toLocaleString()} dates)
          </span>
        </div>
      </div>
      <div style={{ color: P.muted, fontSize: 10, textAlign: "right" }}>
        Data is<br />partial
      </div>
    </div>
  );
}

// ── Main dashboard ─────────────────────────────────────────────────────────────

export default function UsersDashboard() {
  const {
    data: summary, loading: summaryLoading, error: summaryError, refetch,
  } = useApiData("/api/users/summary");

  const [period, setPeriod] = useState("30d");
  const periodDays = parseInt(period, 10);

  const { data: timelineRaw, loading: timelineLoading } =
    useApiData(`/api/users/timeline?period=${periodDays}`);

  const { data: topVenuesRaw } = useApiData("/api/users/top_venues");

  // Stacked bar chart data
  const timelineData = useMemo(() => {
    if (!timelineRaw || !Array.isArray(timelineRaw)) return [];
    return timelineRaw.map((d) => ({
      date: d.date.slice(5), // MM-DD
      ...Object.fromEntries(DEXES.map((dex) => [dex, d[dex] || 0])),
    }));
  }, [timelineRaw]);

  // Donut chart data
  const donutData = useMemo(() => {
    if (!topVenuesRaw || !Array.isArray(topVenuesRaw)) return [];
    return topVenuesRaw
      .filter((v) => v.unique_users > 0)
      .map((v) => ({
        name:         v.dex,
        value:        v.unique_users,
        pct:          v.pct,
        fill:         DEX_COLORS[v.dex] || "#888",
      }));
  }, [topVenuesRaw]);

  // Tick interval for x-axis
  const xInterval = useMemo(() => {
    if (periodDays <= 7)  return 0;
    if (periodDays <= 30) return 4;
    return 9;
  }, [periodDays]);

  if (summaryLoading && !summary) return <Loading message="Loading users data..." />;
  if (summaryError && !summary)   return <ErrorState error={summaryError} onRetry={refetch} />;

  const s = summary || {};
  const bootstrapStatus   = s.bootstrap_status || {};
  const totalUsers        = s.total_unique_users || 0;
  const newUsers          = s.new_users || {};
  const byDex             = s.by_dex || {};

  return (
    <div style={{
      background: P.bg, color: P.text, minHeight: "100vh",
      fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px",
    }}>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>
          HIP-3 User Onboarding
        </h1>
        <p style={{ color: P.muted, fontSize: 11, margin: "4px 0 0" }}>
          New users trading HIP-3 assets — tracking migration from TradFi
        </p>
      </div>

      {/* Bootstrap banner */}
      <BootstrapBanner status={bootstrapStatus} />

      {/* Top row: 4 stat cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 16 }}>
        <StatCard
          title="Total Users"
          value={fmtN(totalUsers)}
          subtitle="All-time unique HIP-3 traders"
          accentColor="#a78bfa"
          large
        />
        <StatCard
          title="New (7d)"
          value={fmtN(newUsers["7d"])}
          subtitle="First-time traders in 7 days"
          accentColor={DEX_COLORS.km}
        />
        <StatCard
          title="New (30d)"
          value={fmtN(newUsers["30d"])}
          subtitle="First-time traders in 30 days"
          accentColor={DEX_COLORS.xyz}
        />
        <StatCard
          title="New (90d)"
          value={fmtN(newUsers["90d"])}
          subtitle="First-time traders in 90 days"
          accentColor={DEX_COLORS.hyna}
        />
      </div>

      {/* Period selector for bar chart */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
        <span style={{ color: P.muted, fontSize: 10, fontWeight: 600, textTransform: "uppercase" }}>Period</span>
        <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${P.border}` }}>
          {PERIODS.map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              style={{
                background: "transparent",
                color: period === p ? "#a78bfa" : P.muted,
                border: "none",
                borderBottom: period === p ? "2px solid #a78bfa" : "2px solid transparent",
                padding: "6px 14px", fontSize: 11, fontWeight: 600,
                cursor: "pointer", fontFamily: "inherit",
              }}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      {/* Second row: stacked bar chart */}
      <div style={{
        background: P.card, border: `1px solid ${P.border}`,
        borderRadius: 10, padding: 20, marginBottom: 12,
      }}>
        <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
          New Users per Day
        </h3>
        <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
          First-time HIP-3 traders · stacked by venue · last {period}
        </p>
        {timelineLoading && !timelineRaw ? (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 260, color: P.muted, fontSize: 11 }}>
            Loading chart data...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={timelineData} barCategoryGap="20%">
              <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} vertical={false} />
              <XAxis
                dataKey="date"
                tick={{ fill: P.muted, fontSize: 9 }}
                tickLine={false}
                interval={xInterval}
              />
              <YAxis
                tick={{ fill: P.muted, fontSize: 9 }}
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
              />
              <Tooltip content={<BarTip />} />
              <Legend
                wrapperStyle={{ fontSize: 10, paddingTop: 8 }}
                formatter={(value) => DEX_NAMES[value] || value}
              />
              {DEXES.map((dex, i) => (
                <Bar
                  key={dex}
                  dataKey={dex}
                  name={dex}
                  fill={DEX_COLORS[dex]}
                  stackId="s"
                  opacity={0.85}
                  barSize={8}
                  radius={i === DEXES.length - 1 ? [2, 2, 0, 0] : [0, 0, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Third row: Donut + venue table */}
      <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 12 }}>

        {/* Donut chart */}
        <div style={{
          background: P.card, border: `1px solid ${P.border}`,
          borderRadius: 10, padding: 20,
        }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
            Users by Venue
          </h3>
          <p style={{ color: P.muted, fontSize: 10, margin: "0 0 8px" }}>
            All-time first venue for each user
          </p>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={donutData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={90}
                paddingAngle={2}
                dataKey="value"
                nameKey="name"
              >
                {donutData.map((entry, i) => (
                  <Cell key={i} fill={entry.fill} opacity={0.9} />
                ))}
              </Pie>
              <Tooltip content={<DonutTip />} />
              <Legend
                wrapperStyle={{ fontSize: 10 }}
                formatter={(value) => DEX_NAMES[value] || value}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Venue breakdown table */}
        <div style={{
          background: P.card, border: `1px solid ${P.border}`,
          borderRadius: 10, padding: 20,
        }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
            Venue Breakdown
          </h3>
          <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
            Unique users onboarded per DEX (all-time)
          </p>

          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${P.border}` }}>
                {["Venue", "Unique Users", "Share", "Bar"].map((h, i) => (
                  <th key={i} style={{
                    padding: "6px 8px",
                    textAlign: i === 0 ? "left" : i === 3 ? "left" : "right",
                    color: P.muted, fontWeight: 600, fontSize: 9,
                    textTransform: "uppercase",
                  }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(topVenuesRaw || []).map((v) => {
                const color = DEX_COLORS[v.dex] || "#888";
                return (
                  <tr key={v.dex} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "8px 8px", fontWeight: 600 }}>
                      <span style={{ color }}>●</span>{" "}
                      <span style={{ color }}>{DEX_NAMES[v.dex] || v.dex}</span>
                    </td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontWeight: 700 }}>
                      {v.unique_users.toLocaleString()}
                    </td>
                    <td style={{ padding: "8px 8px", textAlign: "right", color: P.muted }}>
                      {v.pct}%
                    </td>
                    <td style={{ padding: "8px 8px", minWidth: 100 }}>
                      <div style={{ height: 4, background: P.subtle, borderRadius: 2 }}>
                        <div style={{
                          height: 4, borderRadius: 2,
                          background: color,
                          opacity: 0.75,
                          width: `${v.pct}%`,
                          transition: "width 0.3s ease",
                        }} />
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          {/* By-dex breakdown from summary */}
          <div style={{ marginTop: 20, paddingTop: 16, borderTop: `1px solid ${P.border}` }}>
            <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 10 }}>
              Users per DEX (from user_stats)
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8 }}>
              {DEXES.map((dex) => (
                <div key={dex} style={{
                  background: P.subtle, borderRadius: 6,
                  padding: "8px 10px", borderLeft: `2px solid ${DEX_COLORS[dex]}`,
                }}>
                  <div style={{ color: DEX_COLORS[dex], fontSize: 9, fontWeight: 700, marginBottom: 4 }}>
                    {DEX_NAMES[dex]}
                  </div>
                  <div style={{ fontSize: 15, fontWeight: 700, color: P.text }}>
                    {fmtN(byDex[dex])}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

      </div>

      {/* Footer */}
      <div style={{ marginTop: 16, fontSize: 9, color: P.muted, textAlign: "center" }}>
        Data source: Hydromancer S3 · s3://hydromancer-reservoir/by_dex/{"{dex}"}/fills/perp/all/
        {bootstrapStatus.complete
          ? " · Bootstrap complete"
          : ` · Bootstrap ${bootstrapStatus.processed_dates || 0}/${bootstrapStatus.total_dates || 0} dates`}
      </div>
    </div>
  );
}
