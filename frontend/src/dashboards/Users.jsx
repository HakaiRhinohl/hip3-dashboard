import { useState, useMemo, useCallback } from "react";
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
const PERIODS = ["1d", "7d", "30d", "90d"];
// Log scale slider: 0-100 maps to $0-$10M
const sliderToVol = (s) => {
  if (s <= 0) return 0;
  return Math.round(Math.pow(10, (s / 100) * 7)); // 10^0=1 to 10^7=10M
};
const volToSlider = (v) => {
  if (v <= 0) return 0;
  return Math.round((Math.log10(Math.max(v, 1)) / 7) * 100);
};

const TYPE_A_COLOR = "#a78bfa";
const TYPE_B_COLOR = "#38bdf8";
const NEW_COLOR    = "#34d399";

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
const fmtUSD = (n) => {
  if (n == null) return "—";
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `$${(n / 1_000).toFixed(1)}K`;
  return `$${Number(n).toFixed(0)}`;
};

// ── Tooltips ───────────────────────────────────────────────────────────────────

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

function TabSelector({ options, value, onChange, accentColor = TYPE_A_COLOR }) {
  return (
    <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${P.border}` }}>
      {options.map((opt) => {
        const v = typeof opt === "object" ? opt.value : opt;
        const label = typeof opt === "object" ? opt.label : opt;
        const active = v === value;
        return (
          <button
            key={String(v)}
            onClick={() => onChange(v)}
            style={{
              background: "transparent",
              color: active ? accentColor : P.muted,
              border: "none",
              borderBottom: active ? `2px solid ${accentColor}` : "2px solid transparent",
              padding: "6px 14px", fontSize: 11, fontWeight: 600,
              cursor: "pointer", fontFamily: "inherit",
            }}
          >
            {label}
          </button>
        );
      })}
    </div>
  );
}

// ── Filter Panel ───────────────────────────────────────────────────────────────

const sliderStyle = (color) => ({
  width: "100%", height: 4, appearance: "none", background: P.subtle,
  borderRadius: 2, outline: "none", cursor: "pointer",
  accentColor: color,
});

function FilterPanel({ onApply, loading }) {
  const [filterPeriod, setFilterPeriod] = useState(30);
  const [volSlider, setVolSlider] = useState(0);       // 0-100 log scale
  const [tradfiPct, setTradfiPct] = useState(0);       // 0-100 integer
  const [selectedVenues, setSelectedVenues] = useState(new Set(DEXES));

  const minVol = sliderToVol(volSlider);
  const tradfiRatio = tradfiPct / 100;

  const toggleVenue = (dex) => {
    setSelectedVenues((prev) => {
      const next = new Set(prev);
      next.has(dex) ? next.delete(dex) : next.add(dex);
      return next;
    });
  };

  const handleApply = () => {
    onApply({ period: filterPeriod, min_vol: minVol, tradfi_ratio: tradfiRatio, venues: [...selectedVenues] });
  };

  const periodOpts = PERIODS.map((p) => ({ label: p, value: parseInt(p, 10) }));

  return (
    <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20, marginBottom: 16 }}>
      <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 16px", fontWeight: 600 }}>User Filter</h3>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 24, alignItems: "start" }}>
        {/* Period */}
        <div>
          <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 8, letterSpacing: "0.06em" }}>Period</div>
          <TabSelector options={periodOpts} value={filterPeriod} onChange={setFilterPeriod} accentColor={TYPE_A_COLOR} />
        </div>

        {/* Min Volume — log slider */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
            <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, letterSpacing: "0.06em" }}>Min Volume</div>
            <div style={{ color: "#ffb020", fontSize: 11, fontWeight: 700 }}>{minVol === 0 ? "Any" : fmtUSD(minVol)}</div>
          </div>
          <input type="range" min={0} max={100} step={1} value={volSlider}
            onChange={(e) => setVolSlider(Number(e.target.value))}
            style={sliderStyle("#ffb020")} />
          <div style={{ display: "flex", justifyContent: "space-between", color: P.muted, fontSize: 9, marginTop: 4 }}>
            <span>$0</span><span>$1K</span><span>$100K</span><span>$10M</span>
          </div>
        </div>

        {/* TradFi Ratio — linear slider */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
            <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, letterSpacing: "0.06em" }}>TradFi Ratio ≥</div>
            <div style={{ color: TYPE_A_COLOR, fontSize: 11, fontWeight: 700 }}>{tradfiPct === 0 ? "Any" : `${tradfiPct}%`}</div>
          </div>
          <input type="range" min={0} max={100} step={5} value={tradfiPct}
            onChange={(e) => setTradfiPct(Number(e.target.value))}
            style={sliderStyle(TYPE_A_COLOR)} />
          <div style={{ display: "flex", justifyContent: "space-between", color: P.muted, fontSize: 9, marginTop: 4 }}>
            <span>Any</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span>
          </div>
        </div>

        {/* Venue Selector */}
        <div>
          <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 8, letterSpacing: "0.06em" }}>
            Venues
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {DEXES.map((dex) => {
              const checked = selectedVenues.has(dex);
              return (
                <label
                  key={dex}
                  style={{
                    display: "flex", alignItems: "center", gap: 4,
                    cursor: "pointer", fontSize: 10, fontWeight: 600,
                    color: checked ? DEX_COLORS[dex] : P.muted,
                    padding: "3px 6px",
                    background: checked ? `${DEX_COLORS[dex]}18` : "transparent",
                    border: `1px solid ${checked ? DEX_COLORS[dex] : P.border}`,
                    borderRadius: 4,
                    transition: "all 0.15s ease",
                  }}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleVenue(dex)}
                    style={{ display: "none" }}
                  />
                  {DEX_NAMES[dex]}
                </label>
              );
            })}
          </div>
        </div>
      </div>

      <div style={{ marginTop: 16, display: "flex", alignItems: "center", gap: 12 }}>
        <button
          onClick={handleApply}
          disabled={loading}
          style={{
            background: loading ? P.subtle : TYPE_A_COLOR,
            color: loading ? P.muted : "#0a0020",
            border: "none",
            borderRadius: 6,
            padding: "8px 20px",
            fontSize: 11, fontWeight: 700,
            cursor: loading ? "not-allowed" : "pointer",
            fontFamily: "inherit",
            transition: "all 0.15s ease",
          }}
        >
          {loading ? "Filtering..." : "Apply Filter"}
        </button>
        <div style={{ color: P.muted, fontSize: 9, fontStyle: "italic" }}>
          Proxy based on asset class distribution within HIP-3 trades. Type A = &gt;80% equity/commodity/forex
        </div>
      </div>
    </div>
  );
}

// ── Filter Results ─────────────────────────────────────────────────────────────

function FilterResults({ result, filterParams, timelineData, timelineLoading, xInterval }) {
  if (!result) return null;

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Result stat cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 12 }}>
        <StatCard
          title="Matching Users"
          value={fmtN(result.total_matching)}
          subtitle={`Period: last ${filterParams?.period || 30}d`}
          accentColor={TYPE_A_COLOR}
        />
        <StatCard
          title="Type A Matching"
          value={fmtN(result.type_a_count)}
          subtitle=">80% TradFi volume"
          accentColor={TYPE_A_COLOR}
        />
        <StatCard
          title="Type B Matching"
          value={fmtN(result.type_b_count)}
          subtitle="Crypto-native / mixed"
          accentColor={TYPE_B_COLOR}
        />
        <StatCard
          title="Avg Volume"
          value={fmtUSD(result.avg_volume)}
          subtitle="Per matching user"
          accentColor={NEW_COLOR}
        />
      </div>

      {/* New users per day chart for filtered period */}
      <div style={{
        background: P.card, border: `1px solid ${P.border}`,
        borderRadius: 10, padding: 20, marginBottom: 12,
      }}>
        <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
          New Users per Day (filtered period)
        </h3>
        <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
          First-time HIP-3 traders · stacked by venue · last {filterParams?.period || 30}d
        </p>
        {timelineLoading ? (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 220, color: P.muted, fontSize: 11 }}>
            Loading chart data...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
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

      {/* Sample users table */}
      {result.sample_users && result.sample_users.length > 0 && (
        <div style={{
          background: P.card, border: `1px solid ${P.border}`,
          borderRadius: 10, padding: 20,
        }}>
          <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, margin: "0 0 4px", fontWeight: 600 }}>
            Top Matching Users
          </h3>
          <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
            Top 20 by volume within filter
          </p>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${P.border}` }}>
                {["Address", "Volume", "TradFi Ratio", "First Date", "First DEX"].map((h, i) => (
                  <th key={i} style={{
                    padding: "6px 8px",
                    textAlign: i === 0 ? "left" : "right",
                    color: P.muted, fontWeight: 600, fontSize: 9,
                    textTransform: "uppercase",
                  }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.sample_users.map((u, idx) => {
                const ratio = u.tradfi_ratio || 0;
                const isTypeA = ratio > 0.8;
                const ratioColor = isTypeA ? TYPE_A_COLOR : TYPE_B_COLOR;
                return (
                  <tr key={idx} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "7px 8px", fontFamily: "'IBM Plex Mono', monospace", fontSize: 9, color: P.muted }}>
                      {u.address ? `${u.address.slice(0, 8)}...${u.address.slice(-6)}` : "—"}
                    </td>
                    <td style={{ padding: "7px 8px", textAlign: "right", fontWeight: 700 }}>
                      {fmtUSD(u.volume)}
                    </td>
                    <td style={{ padding: "7px 8px", textAlign: "right" }}>
                      <span style={{
                        color: ratioColor,
                        background: `${ratioColor}18`,
                        borderRadius: 3,
                        padding: "2px 6px",
                        fontWeight: 700,
                        fontSize: 9,
                      }}>
                        {(ratio * 100).toFixed(0)}%
                      </span>
                    </td>
                    <td style={{ padding: "7px 8px", textAlign: "right", color: P.muted }}>
                      {u.first_date || "—"}
                    </td>
                    <td style={{ padding: "7px 8px", textAlign: "right" }}>
                      <span style={{ color: DEX_COLORS[u.first_dex] || P.muted, fontWeight: 700 }}>
                        {DEX_NAMES[u.first_dex] || u.first_dex || "—"}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Main dashboard ─────────────────────────────────────────────────────────────

export default function UsersDashboard() {
  const {
    data: summary, loading: summaryLoading, error: summaryError, refetch,
  } = useApiData("/api/users/summary");

  const { data: typeBreakdown } = useApiData("/api/users/type_breakdown");

  // Period for the bottom stacked bar chart
  const [period, setPeriod] = useState("30d");
  const periodDays = parseInt(period, 10);

  const { data: timelineRaw, loading: timelineLoading } =
    useApiData(`/api/users/timeline?period=${periodDays}`);

  const { data: topVenuesRaw } = useApiData("/api/users/top_venues");

  // Filter state
  const [filterResult, setFilterResult] = useState(null);
  const [filterParams, setFilterParams] = useState(null);
  const [filterLoading, setFilterLoading] = useState(false);

  // Timeline for filtered period
  const [filterPeriodDays, setFilterPeriodDays] = useState(30);
  const { data: filterTimelineRaw, loading: filterTimelineLoading } =
    useApiData(filterParams ? `/api/users/timeline?period=${filterParams.period}` : null);

  const handleApplyFilter = useCallback(async ({ period: p, min_vol, tradfi_ratio, venues }) => {
    setFilterPeriodDays(p);
    setFilterLoading(true);
    try {
      const venueStr = venues.join(",");
      const url = `/api/users/filter?period=${p}&min_vol=${min_vol}&tradfi_ratio=${tradfi_ratio}&venues=${encodeURIComponent(venueStr)}`;
      const apiBase = import.meta.env.VITE_API_URL || "";
      const res = await fetch(`${apiBase}${url}`);
      const data = await res.json();
      setFilterResult(data);
      setFilterParams({ period: p, min_vol, tradfi_ratio, venues });
    } catch (err) {
      console.error("Filter API error:", err);
    } finally {
      setFilterLoading(false);
    }
  }, []);

  // Main stacked bar chart data
  const timelineData = useMemo(() => {
    if (!timelineRaw || !Array.isArray(timelineRaw)) return [];
    return timelineRaw.map((d) => ({
      date: d.date.slice(5), // MM-DD
      ...Object.fromEntries(DEXES.map((dex) => [dex, d[dex] || 0])),
    }));
  }, [timelineRaw]);

  // Filter period timeline data
  const filterTimelineData = useMemo(() => {
    if (!filterTimelineRaw || !Array.isArray(filterTimelineRaw)) return [];
    return filterTimelineRaw.map((d) => ({
      date: d.date.slice(5),
      ...Object.fromEntries(DEXES.map((dex) => [dex, d[dex] || 0])),
    }));
  }, [filterTimelineRaw]);

  // Donut chart data
  const donutData = useMemo(() => {
    if (!topVenuesRaw || !Array.isArray(topVenuesRaw)) return [];
    return topVenuesRaw
      .filter((v) => v.unique_users > 0)
      .map((v) => ({
        name:  v.dex,
        value: v.unique_users,
        pct:   v.pct,
        fill:  DEX_COLORS[v.dex] || "#888",
      }));
  }, [topVenuesRaw]);

  // Tick interval for x-axis
  const xInterval = useMemo(() => {
    if (periodDays <= 7)  return 0;
    if (periodDays <= 30) return 4;
    return 9;
  }, [periodDays]);

  const filterXInterval = useMemo(() => {
    if (filterPeriodDays <= 7)  return 0;
    if (filterPeriodDays <= 30) return 4;
    return 9;
  }, [filterPeriodDays]);

  if (summaryLoading && !summary) return <Loading message="Loading users data..." />;
  if (summaryError && !summary)   return <ErrorState error={summaryError} onRetry={refetch} />;

  const s               = summary || {};
  const bootstrapStatus = s.bootstrap_status || {};
  const totalUsers      = s.total_unique_users || 0;
  const newUsers        = s.new_users || {};
  const byDex           = s.by_dex || {};
  const tb              = typeBreakdown || {};

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
          title="Total HIP-3 Users"
          value={fmtN(totalUsers)}
          subtitle="All-time unique HIP-3 traders"
          accentColor={TYPE_A_COLOR}
          large
        />
        <StatCard
          title="Type A (TradFi Proxy)"
          value={fmtN(tb.type_a)}
          subtitle={`${tb.type_a_pct || 0}% of all users · >80% non-crypto assets`}
          accentColor={TYPE_A_COLOR}
        />
        <StatCard
          title="Type B (Crypto-Native)"
          value={fmtN(tb.type_b)}
          subtitle={`${tb.type_b_pct || 0}% of all users · expanding to HIP-3`}
          accentColor={TYPE_B_COLOR}
        />
        <StatCard
          title="New (30d)"
          value={fmtN(newUsers["30d"])}
          subtitle="First-time traders in 30 days"
          accentColor={NEW_COLOR}
        />
      </div>

      {/* Filter Panel */}
      <FilterPanel onApply={handleApplyFilter} loading={filterLoading} />

      {/* Filter Results */}
      <FilterResults
        result={filterResult}
        filterParams={filterParams}
        timelineData={filterTimelineData}
        timelineLoading={filterTimelineLoading}
        xInterval={filterXInterval}
      />

      {/* Period selector for main bar chart */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
        <span style={{ color: P.muted, fontSize: 10, fontWeight: 600, textTransform: "uppercase" }}>Period</span>
        <TabSelector
          options={["7d", "30d", "90d"]}
          value={period}
          onChange={setPeriod}
          accentColor={TYPE_A_COLOR}
        />
      </div>

      {/* Stacked bar chart */}
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

      {/* Bottom row: Donut + venue table */}
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
        {" · "}Avg TradFi ratio: {tb.avg_tradfi_ratio != null ? `${(tb.avg_tradfi_ratio * 100).toFixed(1)}%` : "—"}
      </div>
    </div>
  );
}
