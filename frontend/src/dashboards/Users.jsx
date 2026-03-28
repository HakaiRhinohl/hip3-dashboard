import { useState, useMemo } from "react";
import {
  BarChart, Bar, ComposedChart,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
  Cell,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_COLORS = { km: "#00e5a0", xyz: "#7c5cfc", flx: "#ff4d6a", cash: "#ffb020" };
const DEX_NAMES  = { km: "Markets", xyz: "Trade.xyz", flx: "Felix", cash: "Dreamcash" };
const TYPE_COLORS = { A: "#a78bfa", B: "#38bdf8" };
const DEXES = ["km", "xyz", "flx", "cash"];

const P = {
  bg:     "#060911",
  card:   "#0c1020",
  border: "#151d38",
  subtle: "#1a2545",
  text:   "#e4eaf3",
  muted:  "#4f5e82",
};

const PERIODS = ["1d", "7d", "30d", "90d"];

// ── Tooltip components ─────────────────────────────────────────────────────────

const UsersTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#0f1530", border: `1px solid ${P.border}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 11,
      boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
    }}>
      <p style={{ color: P.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.filter((p) => p.value > 0).map((p, i) => (
        <p key={i} style={{ color: p.color, margin: "2px 0" }}>
          {p.name}: {p.value.toLocaleString()}
        </p>
      ))}
    </div>
  );
};

const VenueTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#0f1530", border: `1px solid ${P.border}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 11,
      boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
    }}>
      <p style={{ color: P.muted, marginBottom: 4, fontWeight: 600, fontSize: 10 }}>{label}</p>
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.fill, margin: "2px 0" }}>
          {p.name}: {p.value.toLocaleString()} users
        </p>
      ))}
    </div>
  );
};

// ── Summary card ──────────────────────────────────────────────────────────────

function SummaryCard({ title, value, subtitle, accentColor, large }) {
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
      <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 6 }}>
        {title}
      </div>
      <div style={{ fontSize: large ? 28 : 22, fontWeight: 700, color: accentColor, lineHeight: 1.1 }}>
        {value}
      </div>
      {subtitle && (
        <div style={{ color: P.muted, fontSize: 10, marginTop: 4 }}>{subtitle}</div>
      )}
    </div>
  );
}

// ── Main dashboard ─────────────────────────────────────────────────────────────

export default function UsersDashboard() {
  const { data: summary, loading: summaryLoading, error: summaryError, refetch: refetchSummary } =
    useApiData("/api/users");
  const { data: timelineRaw, loading: timelineLoading, error: timelineError } =
    useApiData("/api/users/timeline?days=90");

  const [period, setPeriod] = useState("30d");

  // Period-filtered summary stats
  const periodStats = useMemo(() => {
    if (!summary?.by_period) return null;
    return summary.by_period[period] || null;
  }, [summary, period]);

  // Venue breakdown for horizontal bar chart
  const venueData = useMemo(() => {
    if (!summary?.by_venue) return [];
    return DEXES
      .map((dex) => ({ name: DEX_NAMES[dex], dex, users: summary.by_venue[dex] || 0 }))
      .sort((a, b) => b.users - a.users);
  }, [summary]);

  // Timeline chart data (last 90 days)
  const timelineData = useMemo(() => {
    if (!timelineRaw || !Array.isArray(timelineRaw)) return [];
    return timelineRaw.map((d) => ({
      date: d.date.slice(5),   // MM-DD for display
      "Type A": d.type_a,
      "Type B": d.type_b,
    }));
  }, [timelineRaw]);

  // Loading / error states
  if (summaryLoading && !summary) return <Loading message="Loading users data..." />;
  if (summaryError && !summary)   return <ErrorState error={summaryError} onRetry={refetchSummary} />;
  if (summary?.status === "loading") return <Loading message={summary.message || "Users data not yet available"} />;

  const d = summary || {};

  // Formatted numbers
  const fmtN = (n) => (n == null ? "—" : n.toLocaleString());
  const fmtPct = (n) => (n == null ? "—" : `${n.toFixed(1)}%`);

  // All-time type_a / type_b
  const totalHip3  = d.total_hip3_users || 0;
  const totalTypeA = d.type_a || 0;
  const totalTypeB = d.type_b || 0;
  const tradfiPct  = d.tradfi_pct != null ? d.tradfi_pct : null;

  // Pct breakdowns
  const pctA = totalHip3 > 0 ? (totalTypeA / totalHip3 * 100).toFixed(1) : "0.0";
  const pctB = totalHip3 > 0 ? (totalTypeB / totalHip3 * 100).toFixed(1) : "0.0";

  return (
    <div style={{
      background: P.bg, color: P.text, minHeight: "100vh",
      fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px",
    }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>
          HIP-3 User Tracking
        </h1>
        <p style={{ color: P.muted, fontSize: 11, margin: "4px 0 0" }}>
          New user adoption, TradFi migration analysis — all-time · updated {d.generated_at || "—"}
        </p>
      </div>

      {/* All-time summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 20 }}>
        <SummaryCard
          title="Total HIP-3 Users"
          value={fmtN(totalHip3)}
          subtitle="All-time unique traders"
          accentColor="#a78bfa"
          large
        />
        <SummaryCard
          title="Type A — TradFi Pure"
          value={fmtN(totalTypeA)}
          subtitle={`${pctA}% of HIP-3 users · first HL fill was HIP-3`}
          accentColor={TYPE_COLORS.A}
        />
        <SummaryCard
          title="Type B — HL Adopters"
          value={fmtN(totalTypeB)}
          subtitle={`${pctB}% of HIP-3 users · existing HL user`}
          accentColor={TYPE_COLORS.B}
        />
        <SummaryCard
          title="TradFi Purity %"
          value={fmtPct(tradfiPct)}
          subtitle="Users with >80% volume in HIP-3"
          accentColor="#f472b6"
        />
      </div>

      {/* Period selector + period stats */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 16 }}>
        <span style={{ color: P.muted, fontSize: 10, fontWeight: 600, textTransform: "uppercase" }}>Period</span>
        <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${P.border}` }}>
          {PERIODS.map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              style={{
                background: "transparent",
                color: period === p ? TYPE_COLORS.A : P.muted,
                border: "none",
                borderBottom: period === p ? `2px solid ${TYPE_COLORS.A}` : "2px solid transparent",
                padding: "6px 14px", fontSize: 11, fontWeight: 600,
                cursor: "pointer", fontFamily: "inherit",
              }}
            >
              {p}
            </button>
          ))}
        </div>
        {periodStats && (
          <div style={{ display: "flex", gap: 20, marginLeft: 8, fontSize: 11 }}>
            <span style={{ color: P.muted }}>New users: <span style={{ color: P.text, fontWeight: 600 }}>{fmtN(periodStats.total)}</span></span>
            <span style={{ color: TYPE_COLORS.A }}>Type A: <span style={{ fontWeight: 600 }}>{fmtN(periodStats.type_a)}</span></span>
            <span style={{ color: TYPE_COLORS.B }}>Type B: <span style={{ fontWeight: 600 }}>{fmtN(periodStats.type_b)}</span></span>
          </div>
        )}
      </div>

      {/* Charts row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 12, marginBottom: 12 }}>

        {/* Daily new users — stacked bar */}
        <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20 }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
            New HIP-3 Users per Day
          </h3>
          <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
            Last 90 days · stacked by Type A (TradFi) vs Type B (HL adopter)
          </p>
          {timelineLoading && !timelineRaw ? (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 280, color: P.muted, fontSize: 11 }}>
              Loading timeline...
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <ComposedChart data={timelineData} barCategoryGap="15%">
                <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} vertical={false} />
                <XAxis
                  dataKey="date"
                  tick={{ fill: P.muted, fontSize: 8 }}
                  tickLine={false}
                  interval={13}
                />
                <YAxis
                  tick={{ fill: P.muted, fontSize: 9 }}
                  tickLine={false}
                  axisLine={false}
                  allowDecimals={false}
                />
                <Tooltip content={<UsersTip />} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                <Bar dataKey="Type A" name="Type A — TradFi" fill={TYPE_COLORS.A} stackId="s" opacity={0.85} barSize={6} />
                <Bar dataKey="Type B" name="Type B — HL Adopter" fill={TYPE_COLORS.B} stackId="s" opacity={0.85} barSize={6} radius={[2, 2, 0, 0]} />
              </ComposedChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Venue breakdown — horizontal bars */}
        <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20 }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
            Users by Venue
          </h3>
          <p style={{ color: P.muted, fontSize: 10, margin: "0 0 20px" }}>
            All-time unique traders per DEX
          </p>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart
              data={venueData}
              layout="vertical"
              margin={{ left: 8, right: 24, top: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} horizontal={false} />
              <XAxis type="number" tick={{ fill: P.muted, fontSize: 9 }} tickLine={false} axisLine={false} allowDecimals={false} />
              <YAxis type="category" dataKey="name" tick={{ fill: P.text, fontSize: 10 }} tickLine={false} axisLine={false} width={68} />
              <Tooltip content={<VenueTip />} />
              <Bar dataKey="users" name="Users" radius={[0, 3, 3, 0]} barSize={18}>
                {venueData.map((entry) => (
                  <Cell key={entry.dex} fill={DEX_COLORS[entry.dex]} opacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>

          {/* Venue numbers table */}
          <div style={{ marginTop: 16, borderTop: `1px solid ${P.border}`, paddingTop: 12 }}>
            {venueData.map((entry) => {
              const maxUsers = venueData[0]?.users || 1;
              const pct = (entry.users / maxUsers * 100).toFixed(0);
              return (
                <div key={entry.dex} style={{ marginBottom: 8 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3, fontSize: 10 }}>
                    <span style={{ color: DEX_COLORS[entry.dex], fontWeight: 600 }}>{entry.name}</span>
                    <span style={{ color: P.text, fontWeight: 600 }}>{entry.users.toLocaleString()}</span>
                  </div>
                  <div style={{ height: 3, background: P.subtle, borderRadius: 2 }}>
                    <div style={{
                      height: 3, borderRadius: 2,
                      background: DEX_COLORS[entry.dex],
                      opacity: 0.7,
                      width: `${pct}%`,
                      transition: "width 0.3s ease",
                    }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Type classification explainer */}
      <div style={{
        background: P.card, border: `1px solid ${P.border}`,
        borderRadius: 10, padding: "14px 20px",
        display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16,
        marginBottom: 12,
      }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            <div style={{ width: 8, height: 8, borderRadius: "50%", background: TYPE_COLORS.A }} />
            <span style={{ fontWeight: 600, fontSize: 12, color: TYPE_COLORS.A }}>Type A — TradFi Pure</span>
          </div>
          <p style={{ color: P.muted, fontSize: 10, margin: 0, lineHeight: 1.6 }}>
            Their very first Hyperliquid fill EVER was a HIP-3 asset (within 24 hours of account creation).
            These are TradFi migrants who came directly to HIP-3 without prior crypto trading on HL.
          </p>
        </div>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            <div style={{ width: 8, height: 8, borderRadius: "50%", background: TYPE_COLORS.B }} />
            <span style={{ fontWeight: 600, fontSize: 12, color: TYPE_COLORS.B }}>Type B — HL Adopters</span>
          </div>
          <p style={{ color: P.muted, fontSize: 10, margin: 0, lineHeight: 1.6 }}>
            Existing Hyperliquid users (traded crypto perps for more than 1 day) who subsequently tried
            a HIP-3 asset. These are crypto-native traders adopting tokenised real-world assets.
          </p>
        </div>
      </div>

      {/* Footer note */}
      <div style={{ fontSize: 9, color: P.muted, textAlign: "center", marginTop: 4 }}>
        Based on historical S3 fills data from Hyperliquid — bootstrap updated{" "}
        <span style={{ color: P.text }}>{d.bootstrap_date || "unknown"}</span>
        {" "}· Incremental updates every 1 hour
      </div>
    </div>
  );
}
