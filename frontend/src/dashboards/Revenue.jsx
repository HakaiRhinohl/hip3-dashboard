import { useState, useMemo } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, ComposedChart, Line,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_META = {
  km: { name: "Kinetiq Markets", short: "Kinetiq", color: "#00e5a0" },
  xyz: { name: "Trade.xyz", short: "Trade.xyz", color: "#7c5cfc" },
  flx: { name: "Felix", short: "Felix", color: "#ff4d6a" },
  cash: { name: "Dreamcash", short: "Dreamcash", color: "#ffb020" },
};

// km normal-mode rate (hypothetical — what km would earn if growth mode were disabled)
const KM_NORMAL_BPS = 4.0743;
const MIGRATION_DATE = "2026-06-20";

const KINETIQ_ONCHAIN_FALLBACK = {
  as_of: "2026-09-02",
  user_fees: 924520,
  hip3_fees: 728060,
  deployer_revenue: 338960,
  builder_revenue: 196450,
  protocol_revenue: 535410,
  kmhype_allocation: 33900,
  minimum_kntq_buybacks: 230300,
  operations_reinvestment: 271200,
};

const KHYPE_REVENUE = {
  protocolRevenue: 2466790,
  grossStakingYield: 26610000,
  treasury: 1910000,
  buybacks: 553800,
  quarters: [
    { label: "Q3 '25", value: 93000 },
    { label: "Q4 '25", value: 1370000 },
    { label: "Q1 '26", value: 191440 },
    { label: "Q2 '26", value: 467670 },
    { label: "Q3 '26*", value: 344680 },
  ],
};

const KNTQ_BURN_FLOW = {
  deployer: "0x51172933b60847085e2a959e860e2ec9e240ac09",
  assistanceFund: "0xfefefefefefefefefefefefefefefefefefefefe",
  explorer: "https://hypurrscan.io/address/0x51172933b60847085e2a959e860e2ec9e240ac09#txs",
  observedTransfers: 40,
};

const WALLETS = [
  { role: "Markets fee recipient", address: "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2" },
  { role: "Markets builder", address: "0x42f3226007290b02c5a0b15bccbb1ba6df04f992" },
  { role: "kmHYPE StakingManager", address: "0x71f0019cc7fa79e4f42587fb7b9a817d8d2429ec" },
  { role: "sKNTQ buybacks", address: "0xaa3b7392052d62928cc87701e3ca6fb6630bb6e2" },
  { role: "kHYPE treasury", address: "0x64bD77698Ab7C3Fd0a1F54497b228ED7a02098E3" },
  { role: "KNTQ spot deployer", address: KNTQ_BURN_FLOW.deployer },
  { role: "Assistance Fund", address: KNTQ_BURN_FLOW.assistanceFund },
];

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

function AllocationBar({ label, value, total, color, note }) {
  const width = total > 0 ? Math.min(100, (value / total) * 100) : 0;
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 6, fontSize: 10 }}>
        <span style={{ color: C.text }}>{label}</span>
        <span style={{ color }}>{fmt(value)}</span>
      </div>
      <div style={{ height: 6, background: C.bg, borderRadius: 10, overflow: "hidden" }}>
        <div style={{ width: `${width}%`, height: "100%", background: color, borderRadius: 10 }} />
      </div>
      {note && <div style={{ color: C.muted, fontSize: 9, marginTop: 5 }}>{note}</div>}
    </div>
  );
}

function WalletRow({ role, address }) {
  return (
    <div className="wallet-row" style={{ padding: "9px 0", borderBottom: `1px solid ${C.border}`, display: "grid", gridTemplateColumns: "160px 1fr", gap: 12, fontSize: 10 }}>
      <span style={{ color: C.muted }}>{role}</span>
      <span style={{ color: C.text, overflowWrap: "anywhere" }}>{address}</span>
    </div>
  );
}

export default function RevenueDashboard({ dexId = "km" }) {
  const { data: revData, loading: revLoading, error: revError, refetch: revRefetch } =
    useApiData(`/api/revenue?dex=${dexId}`);

  const [tab, setTab] = useState("revenue");

  const meta = DEX_META[dexId] || DEX_META.km;
  const accent = meta.color;
  const isKm = dexId === "km";

  const dexData = useMemo(() => {
    if (!revData || revData.status === "loading") return null;
    return {
      dex: revData.dex || dexId,
      num_tickers: revData.num_tickers || 0,
      num_days: revData.days_since_launch || 0,
      cum_volume: revData.kpis?.cumulative_volume ?? revData.total_volume ?? 0,
      deployer_fees: revData.fees?.deployer || 0,
      builder_fees: revData.fees?.builder || 0,
      total_fees: revData.kpis?.protocol_revenue ?? revData.fees?.total ?? 0,
      eff_deployer_bps: revData.rates?.eff_deployer_bps_growth || 0,
      eff_builder_bps: revData.rates?.eff_builder_bps || 0,
      eff_total_bps: revData.kpis?.effective_total_bps ?? revData.rates?.eff_total_bps ?? ((revData.rates?.eff_deployer_bps_growth || 0) + (revData.rates?.eff_builder_bps || 0)),
      normal_deployer_bps: revData.rates?.eff_deployer_bps_normal || 0,
      annualized_revenue: revData.kpis?.annualized_revenue,
      annualized_normal_revenue: revData.kpis?.annualized_normal_revenue,
      total_net_deposit: revData.total_net_deposit || 0,
      avg_7d: revData.averages?.avg_7d || 0,
      avg_30d: revData.averages?.avg_30d || 0,
      top_tickers: revData.top_tickers || revData.ticker_chart || [],
    };
  }, [revData, dexId]);

  const chartData = useMemo(() => {
    if (!revData?.daily_chart) return [];
    const rows = revData.daily_chart.map((d) => ({ ...d }));
    if (isKm && !rows.some((d) => d.date === MIGRATION_DATE)) {
      const previous = [...rows].reverse().find((d) => d.date < MIGRATION_DATE);
      rows.push({
        date: MIGRATION_DATE,
        daily_volume_usd: 0,
        cum_volume_usd: previous?.cum_volume_usd || 0,
        deployer_fee_growth: 0,
        builder_fee: 0,
        total_fee_growth: 0,
        era: "legacy",
      });
      rows.sort((a, b) => a.date.localeCompare(b.date));
    }
    return rows;
  }, [revData, isKm]);

  const feeChartData = useMemo(() => {
    let cum = 0;
    return chartData.map((d) => {
      const totalFee = d.total_fee_growth ?? ((d.deployer_fee_growth || 0) + (d.builder_fee || 0));
      cum += totalFee;
      return { ...d, cum_total_g: cum };
    });
  }, [chartData]);

  const loading = revLoading;
  const error = revError;

  if (loading) return <Loading message={`Loading ${meta.short} data...`} />;
  if (error && !dexData) return <ErrorState error={error} onRetry={revRefetch} />;
  if (!dexData) return <Loading message={`Waiting for ${meta.short} data...`} />;

  const d = dexData;
  const fees = { deployer: d.deployer_fees || 0, builder: d.builder_fees || 0, total: d.total_fees || 0 };
  const deployerBps = d.eff_deployer_bps || 0;
  const builderBps = d.eff_builder_bps || 0;
  const effBps = d.eff_total_bps || deployerBps + builderBps;
  const normalBps = isKm ? (d.normal_deployer_bps || KM_NORMAL_BPS) : 0;
  const avg7d = d.avg_7d || 0;
  const activeDays = d.num_days || 0;

  // Annualized average since launch, based on realized cumulative fees.
  const annDeployer = activeDays > 0 ? (fees.deployer / activeDays) * 365 : 0;
  const annBuilder = activeDays > 0 ? (fees.builder / activeDays) * 365 : 0;
  const annTotal = d.annualized_revenue ?? (annDeployer + annBuilder);
  const normalMultiplier = isKm && deployerBps > 0 ? (normalBps / deployerBps) : 0;
  const fallbackAnnNormal = isKm ? annDeployer * normalMultiplier + annBuilder : 0;
  const annNormalTotal = isKm ? (d.annualized_normal_revenue ?? fallbackAnnNormal) : 0;
  const annNormalDeployer = isKm ? Math.max(0, annNormalTotal - annBuilder) : 0;
  const annualizedBreakdown = isKm
    ? [
      { name: "Actual", deployer: annDeployer, builder: annBuilder },
      { name: "Normal", deployer: annNormalDeployer, builder: annBuilder },
    ]
    : [
      { name: "Actual", deployer: annDeployer, builder: annBuilder },
    ];

  const pieData = [
    { name: "Deployer Fees", value: fees.deployer, color: C.amber },
    { name: "Builder Fees", value: fees.builder, color: C.purple },
  ].filter((p) => p.value > 0);

  const tickerData = (d.top_tickers || []).map((ticker) => ({
    ...ticker,
    displayTicker: isKm && ticker.quote ? `${ticker.ticker} · ${ticker.quote}` : ticker.ticker,
  }));

  const reconstruction = revData?.onchain_reconstruction || KINETIQ_ONCHAIN_FALLBACK;
  const migration = revData?.migration || {
    cutoff: MIGRATION_DATE,
    legacy: { dex: "km", quote: "USDH", last_day: MIGRATION_DATE },
    current: { dex: "mkts", quote: "USDC", first_day: "2026-06-21" },
  };

  const tabs = [
    { id: "revenue", label: "Revenue" },
    ...(isKm ? [{ id: "lst", label: "LST Revenue" }] : []),
    { id: "volume", label: "Volume" },
    { id: "breakdown", label: "Breakdown" },
    { id: "tickers", label: "Tickers" },
  ];

  return (
    <div className="revenue-page" style={{ background: C.bg, color: C.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <style>{`
        .revenue-kpis { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); }
        .migration-grid { display: grid; grid-template-columns: 1fr auto 1fr; }
        .lst-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }
        .lst-bottom-grid { display: grid; grid-template-columns: 1.05fr 0.95fr; }
        @media (max-width: 1050px) {
          .revenue-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          .lst-grid, .lst-bottom-grid, .burn-flow-grid { grid-template-columns: 1fr !important; }
        }
        @media (max-width: 680px) {
          .revenue-page { padding: 14px 12px !important; }
          .revenue-kpis { grid-template-columns: 1fr; }
          .migration-grid { grid-template-columns: 1fr !important; }
          .migration-arrow { min-width: 0 !important; flex-direction: row !important; gap: 8px; }
          .migration-current { text-align: left !important; }
          .revenue-header { flex-wrap: wrap; gap: 10px; }
          .revenue-tabs { overflow-x: auto; }
          .revenue-tabs button { padding: 8px 11px !important; white-space: nowrap; }
          .revenue-content { padding: 14px !important; }
          .wallet-row { grid-template-columns: 1fr !important; gap: 4px !important; }
          .burn-wallet-flow { grid-template-columns: 1fr !important; }
          .breakdown-grid { grid-template-columns: 1fr !important; }
        }
      `}</style>
      {/* Header */}
      <div className="revenue-header" style={{ marginBottom: 24, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 8, height: 8, borderRadius: "50%", background: accent, boxShadow: `0 0 12px ${accent}` }} />
            <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>{meta.name}</h1>
          </div>
          <p style={{ color: C.muted, fontSize: 11, margin: "4px 0 0 18px" }}>
            Revenue Analysis · on-chain primary · updated {revData?.generated_at || "..."}
          </p>
        </div>
        <div style={{ background: `${accent}0d`, border: `1px solid ${accent}44`, borderRadius: 6, padding: "6px 12px", fontSize: 10, color: accent }}>
          {isKm ? "ON-CHAIN RECONSTRUCTION" : `${d.num_tickers} tickers · ${d.num_days} days`}
        </div>
      </div>

      {/* KPIs */}
      <div className="revenue-kpis" style={{ gap: 10, marginBottom: 20 }}>
        <StatCard label="Cumulative Volume" value={fmt(d.cum_volume)} sub={`${fmt(avg7d)}/day · 7d calendar avg`} accent={C.cyan} />
        <StatCard label={isKm ? "Protocol Revenue" : "Total Fees"} value={fmt(fees.total)} sub={fees.builder > 0 ? `${fmt(fees.deployer)} deployer + ${fmt(fees.builder)} builder` : `${fmt(fees.deployer)} deployer`} accent={C.amber} />
        <StatCard label={isKm ? "Effective Take Rate" : "Effective Rate"} value={effBps > 0 ? `${effBps.toFixed(2)} bps` : "—"} sub={isKm ? `${deployerBps.toFixed(2)} deployer + ${builderBps.toFixed(2)} builder` : "Deployer + builder"} accent={accent} />
        <StatCard label="Ann. Revenue" value={annTotal > 0 ? fmt(annTotal) : "—"} sub={annTotal > 0 ? `${fmt(annTotal / 12)}/mo · historical avg` : ""} accent={accent} />
        {isKm ? (
          <StatCard label="Ann. Revenue (Normal)" value={annNormalTotal > 0 ? fmt(annNormalTotal) : "—"} sub={annNormalTotal > 0 ? `${fmt(annNormalTotal / 12)}/mo · ${normalBps.toFixed(2)} deployer bps` : ""} accent={C.purple} />
        ) : (
          <StatCard label="Net Deposit" value={fmt(d.total_net_deposit)} sub="Total deposited in DEX" accent={C.purple} />
        )}
      </div>

      {isKm && (
        <>
          <div className="migration-grid" style={{ gap: 14, alignItems: "stretch", background: "linear-gradient(110deg, #10172a, #0b1020)", border: `1px solid ${C.borderLight}`, borderRadius: 10, padding: 16, marginBottom: 12 }}>
            <div>
              <div style={{ color: C.muted, fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>Legacy era</div>
              <div style={{ fontFamily: "'IBM Plex Sans'", fontSize: 17, fontWeight: 700, marginTop: 4 }}>km · USDH</div>
              <div style={{ color: C.muted, fontSize: 10, marginTop: 4 }}>22 markets settled · through {migration.legacy.last_day}</div>
            </div>
            <div className="migration-arrow" style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: C.amber, minWidth: 140 }}>
              <div style={{ fontSize: 9, letterSpacing: "0.08em" }}>MIGRATION</div>
              <div style={{ fontSize: 22, lineHeight: 1 }}>→</div>
              <div style={{ fontSize: 10 }}>{migration.cutoff}</div>
            </div>
            <div className="migration-current" style={{ textAlign: "right" }}>
              <div style={{ color: C.muted, fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase" }}>Current era</div>
              <div style={{ fontFamily: "'IBM Plex Sans'", fontSize: 17, fontWeight: 700, marginTop: 4, color: accent }}>mkts · USDC</div>
              <div style={{ color: C.muted, fontSize: 10, marginTop: 4 }}>New market set · from {migration.current.first_day}</div>
            </div>
          </div>

          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 20 }}>
            {[
              ["Trader fees", reconstruction.user_fees],
              ["HIP-3 fees", reconstruction.hip3_fees],
              ["Captured revenue", reconstruction.protocol_revenue],
              ["Min. KNTQ buybacks", reconstruction.minimum_kntq_buybacks],
            ].map(([label, value]) => (
              <div key={label} style={{ flex: "1 1 160px", background: C.card, border: `1px solid ${C.border}`, borderRadius: 7, padding: "10px 12px" }}>
                <div style={{ color: C.muted, fontSize: 9, textTransform: "uppercase" }}>{label}</div>
                <div style={{ color: label === "Captured revenue" ? accent : C.text, fontSize: 15, fontWeight: 700, marginTop: 3 }}>{fmt(value)}</div>
              </div>
            ))}
            <div style={{ flexBasis: "100%", color: C.muted, fontSize: 9 }}>
              Audited snapshot {reconstruction.as_of}. Volume = daily base volume × close; take rate = captured revenue ÷ volume; annualization = historical daily average × 365. DefiLlama is excluded when it conflicts with transaction-level flows.
            </div>
          </div>
        </>
      )}

      {/* Tabs */}
      <div className="revenue-tabs" style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: `1px solid ${C.border}` }}>
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            background: "transparent", color: tab === t.id ? accent : C.muted,
            border: "none", borderBottom: tab === t.id ? `2px solid ${accent}` : "2px solid transparent",
            padding: "8px 16px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit",
          }}>{t.label}</button>
        ))}
      </div>

      <div className="revenue-content" style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 20, minHeight: 420 }}>
        {tab === "revenue" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: 0, fontWeight: 600 }}>Daily Revenue</h3>
              <div style={{ fontSize: 10, color: C.muted }}>Bars = allocated daily · Line = cumulative</div>
            </div>
            {feeChartData.length > 0 ? (
              <ResponsiveContainer width="100%" height={340}>
                <ComposedChart data={feeChartData} barGap={1}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                  <XAxis dataKey="date" tickFormatter={(value) => value.slice(5)} tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={3} />
                  <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                  <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                  <Bar yAxisId="d" dataKey="deployer_fee_growth" name="Deployer Fee" fill={accent} stackId="g" opacity={0.85} />
                  {fees.builder > 0 && <Bar yAxisId="d" dataKey="builder_fee" name="Builder Fee" fill={C.cyan} stackId="g" opacity={0.85} radius={[2, 2, 0, 0]} />}
                  <Line yAxisId="c" type="monotone" dataKey="cum_total_g" name="Cumulative" stroke={accent} strokeWidth={2} dot={false} />
                  {isKm && <ReferenceLine yAxisId="d" x={MIGRATION_DATE} stroke={C.amber} strokeDasharray="4 3" label={{ value: "USDH → USDC", fill: C.amber, fontSize: 9, position: "insideTopRight" }} />}
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 340, display: "flex", alignItems: "center", justifyContent: "center", color: C.muted, fontSize: 12 }}>
                No fee data available for {meta.short}
              </div>
            )}
          </div>
        )}

        {tab === "lst" && isKm && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", marginBottom: 18 }}>
              <div>
                <div style={{ color: accent, fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 5 }}>Liquid staking revenue</div>
                <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 18, margin: 0, fontWeight: 700 }}>kHYPE and kmHYPE are separate flows</h3>
              </div>
              <div style={{ color: C.muted, fontSize: 9, maxWidth: 340, textAlign: "right", lineHeight: 1.5 }}>
                Protocol revenue uses fee-recipient and transaction flows. Gross staking yield is shown only as context and is not counted as revenue.
              </div>
            </div>

            <div className="lst-grid" style={{ gap: 14, marginBottom: 14 }}>
              <div style={{ background: "linear-gradient(145deg, #101b29, #0b1020)", border: `1px solid ${accent}40`, borderRadius: 9, padding: 18 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                  <div>
                    <div style={{ color: accent, fontSize: 18, fontWeight: 700 }}>kHYPE</div>
                    <div style={{ color: C.muted, fontSize: 10, marginTop: 2 }}>10% performance fee</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <div style={{ color: C.muted, fontSize: 9, textTransform: "uppercase" }}>Historical revenue</div>
                    <div style={{ color: C.text, fontSize: 23, fontWeight: 700 }}>{fmt(KHYPE_REVENUE.protocolRevenue)}</div>
                  </div>
                </div>
                <div style={{ marginTop: 18 }}>
                  <AllocationBar label="Treasury / retained" value={KHYPE_REVENUE.treasury} total={KHYPE_REVENUE.protocolRevenue} color={C.amber} note="Historical reconstructed destination" />
                  <AllocationBar label="KNTQ buybacks" value={KHYPE_REVENUE.buybacks} total={KHYPE_REVENUE.protocolRevenue} color={accent} note="Formula-based historical estimate" />
                </div>
                <div style={{ background: C.bg, borderRadius: 6, padding: "9px 11px", color: C.muted, fontSize: 9, lineHeight: 1.55 }}>
                  Current policy since 2026-04-09: 70% of the performance fee to KNTQ buybacks and 30% to treasury. Gross staking yield reference: {fmt(KHYPE_REVENUE.grossStakingYield)}.
                </div>
              </div>

              <div style={{ background: "linear-gradient(145deg, #171629, #0b1020)", border: `1px solid ${C.purple}55`, borderRadius: 9, padding: 18 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                  <div>
                    <div style={{ color: C.purple, fontSize: 18, fontWeight: 700 }}>kmHYPE</div>
                    <div style={{ color: C.muted, fontSize: 10, marginTop: 2 }}>Markets-linked LST</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <div style={{ color: C.muted, fontSize: 9, textTransform: "uppercase" }}>Markets allocation</div>
                    <div style={{ color: C.text, fontSize: 23, fontWeight: 700 }}>{fmt(reconstruction.kmhype_allocation)}</div>
                  </div>
                </div>
                <div style={{ marginTop: 18 }}>
                  <AllocationBar label="kmHYPE share" value={reconstruction.kmhype_allocation} total={reconstruction.deployer_revenue} color={C.purple} note="10% of reconstructed deployer revenue" />
                  <AllocationBar label="Minimum KNTQ buybacks" value={reconstruction.minimum_kntq_buybacks} total={reconstruction.protocol_revenue} color={accent} note="Builder revenue + a separate 10% deployer allocation" />
                  <AllocationBar label="Operations / reinvestment" value={reconstruction.operations_reinvestment} total={reconstruction.protocol_revenue} color={C.cyan} note="Residual reconstructed allocation" />
                </div>
                <div style={{ background: C.bg, borderRadius: 6, padding: "9px 11px", color: C.muted, fontSize: 9, lineHeight: 1.55 }}>
                  This is not the full Markets revenue. It is the portion attributable to kmHYPE from the deployer flow.
                </div>
              </div>
            </div>

            <div style={{ background: "linear-gradient(110deg, #172319, #0b1320 58%, #15141d)", border: `1px solid ${accent}55`, borderRadius: 9, padding: 18, marginBottom: 14, position: "relative", overflow: "hidden" }}>
              <div style={{ position: "absolute", inset: "0 0 auto", height: 2, background: `linear-gradient(90deg, ${accent}, ${C.amber}, transparent)` }} />
              <div className="burn-flow-grid" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1.2fr) minmax(300px, 0.8fr)", gap: 24, alignItems: "center" }}>
                <div>
                  <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 8, marginBottom: 8 }}>
                    <div style={{ color: accent, fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", fontWeight: 700 }}>Additional KNTQ sink</div>
                    <div style={{ color: C.amber, background: `${C.amber}14`, border: `1px solid ${C.amber}44`, borderRadius: 20, padding: "3px 8px", fontSize: 8, letterSpacing: "0.08em" }}>NOT INCLUDED IN MARKETS REVENUE</div>
                  </div>
                  <div style={{ fontFamily: "'IBM Plex Sans'", fontSize: 18, fontWeight: 700, marginBottom: 7 }}>KNTQ spot fees → Assistance Fund burn</div>
                  <div style={{ color: C.muted, fontSize: 10, lineHeight: 1.65, maxWidth: 720 }}>
                    KNTQ earned by the token's spot deployer is transferred recurrently to Hyperliquid's Assistance Fund as a KNTQ burn flow. This is separate from Markets protocol revenue and from the sKNTQ buyback wallet.
                  </div>
                  <a href={KNTQ_BURN_FLOW.explorer} target="_blank" rel="noreferrer" style={{ display: "inline-block", color: accent, fontSize: 9, marginTop: 10, textDecoration: "none", borderBottom: `1px solid ${accent}66`, paddingBottom: 2 }}>
                    Verify full deployer history on Hypurrscan ↗
                  </a>
                </div>
                <div style={{ background: `${C.bg}cc`, border: `1px solid ${C.border}`, borderRadius: 7, padding: 13 }}>
                  <div style={{ color: C.muted, fontSize: 8, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 10 }}>{KNTQ_BURN_FLOW.observedTransfers}+ outbound KNTQ transfers verified</div>
                  <div className="burn-wallet-flow" style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: 10, alignItems: "center" }}>
                    <div>
                      <div style={{ color: C.muted, fontSize: 8, marginBottom: 4 }}>SPOT DEPLOYER</div>
                      <div style={{ color: C.text, fontSize: 9, overflowWrap: "anywhere" }}>{KNTQ_BURN_FLOW.deployer}</div>
                    </div>
                    <div style={{ color: accent, fontSize: 18 }}>→</div>
                    <div>
                      <div style={{ color: C.muted, fontSize: 8, marginBottom: 4 }}>ASSISTANCE FUND</div>
                      <div style={{ color: C.text, fontSize: 9, overflowWrap: "anywhere" }}>{KNTQ_BURN_FLOW.assistanceFund}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="lst-bottom-grid" style={{ gap: 14 }}>
              <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: 16 }}>
                <div style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, fontWeight: 600, marginBottom: 14 }}>kHYPE revenue by quarter</div>
                {KHYPE_REVENUE.quarters.map((quarter) => (
                  <div key={quarter.label} style={{ display: "grid", gridTemplateColumns: "62px 1fr 72px", gap: 10, alignItems: "center", marginBottom: 10, fontSize: 10 }}>
                    <span style={{ color: C.muted }}>{quarter.label}</span>
                    <div style={{ background: C.card, height: 7, borderRadius: 10, overflow: "hidden" }}>
                      <div style={{ width: `${(quarter.value / 1370000) * 100}%`, height: "100%", background: `linear-gradient(90deg, ${accent}, ${C.cyan})`, borderRadius: 10 }} />
                    </div>
                    <span style={{ color: C.text, textAlign: "right" }}>{fmt(quarter.value)}</span>
                  </div>
                ))}
                <div style={{ color: C.muted, fontSize: 9, marginTop: 12 }}>* Q3 2026 partial through the audited snapshot.</div>
              </div>

              <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: 16 }}>
                <div style={{ fontFamily: "'IBM Plex Sans'", fontSize: 13, fontWeight: 600, marginBottom: 5 }}>Observed wallets</div>
                {WALLETS.map((wallet) => <WalletRow key={wallet.address} {...wallet} />)}
              </div>
            </div>
          </div>
        )}

        {tab === "volume" && (
          <div>
            <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Daily Trading Volume</h3>
            <ResponsiveContainer width="100%" height={340}>
              <ComposedChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} />
                <XAxis dataKey="date" tickFormatter={(value) => value.slice(5)} tick={{ fill: C.muted, fontSize: 9 }} tickLine={false} interval={3} />
                <YAxis yAxisId="d" orientation="left" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <YAxis yAxisId="c" orientation="right" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} tickLine={false} axisLine={false} />
                <Tooltip content={<Tip />} /><Legend wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
                <Bar yAxisId="d" dataKey="daily_volume_usd" name="Daily Volume" fill={accent} opacity={0.6} radius={[2, 2, 0, 0]} />
                <Line yAxisId="c" type="monotone" dataKey="cum_volume_usd" name="Cumulative" stroke={C.amber} strokeWidth={2} dot={false} />
                <ReferenceLine yAxisId="d" y={avg7d} stroke={accent} strokeDasharray="6 3" strokeWidth={1} label={{ value: "7d avg", fill: accent, fontSize: 9, position: "right" }} />
                {isKm && <ReferenceLine yAxisId="d" x={MIGRATION_DATE} stroke={C.amber} strokeDasharray="4 3" label={{ value: "USDH → USDC", fill: C.amber, fontSize: 9, position: "insideTopRight" }} />}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {tab === "breakdown" && (
          <div className="breakdown-grid" style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
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
              <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 16px", fontWeight: 600 }}>Annualized Revenue (avg since launch)</h3>
              {annTotal > 0 ? (
                <>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={annualizedBreakdown} layout="vertical" barCategoryGap="25%">
                      <CartesianGrid strokeDasharray="3 3" stroke={C.subtle} opacity={0.3} horizontal={false} />
                      <XAxis type="number" tick={{ fill: C.muted, fontSize: 9 }} tickFormatter={fmt} />
                      <YAxis type="category" dataKey="name" tick={{ fill: C.text, fontSize: 11, fontWeight: 500 }} width={70} />
                      <Tooltip formatter={(v) => fmt(v)} /><Legend wrapperStyle={{ fontSize: 10 }} />
                      <Bar dataKey="deployer" name="Deployer" fill={C.amber} stackId="a" />
                      <Bar dataKey="builder" name="Builder" fill={C.purple} stackId="a" radius={[0, 3, 3, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div style={{ marginTop: 16, padding: "10px 14px", background: C.bg, borderRadius: 6, fontSize: 11, color: C.muted, textAlign: "center" }}>
                    Actual: <strong style={{ color: accent }}>{fmt(annTotal)}/yr</strong>
                    {isKm && annNormalTotal > 0 && (
                      <>
                        {" → Normal: "}<strong style={{ color: C.purple }}>{fmt(annNormalTotal)}/yr</strong>
                        {annTotal > 0 && <> = <strong style={{ color: C.amber }}>{(annNormalTotal / annTotal).toFixed(1)}x</strong></>}
                      </>
                    )}
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
                  <YAxis type="category" dataKey="displayTicker" tick={{ fill: C.text, fontSize: 10 }} width={125} />
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
        Hyperliquid L1 API · Transaction-level reconstruction · Auto-refresh every 5 min · {revData?.generated_at}
      </div>
    </div>
  );
}
