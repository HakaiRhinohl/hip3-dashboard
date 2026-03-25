import { useState, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useApiData } from "../hooks/useApiData";
import { Loading, ErrorState } from "../components/States";

const DEX_META = {
  km: { name: "Kinetiq", color: "#00e5a0" },
  xyz: { name: "Trade.xyz", color: "#7c5cfc" },
  flx: { name: "Felix", color: "#ff4d6a" },
  cash: { name: "Dreamcash", color: "#ffb020" },
};
const DEXES = ["km", "xyz", "flx", "cash"];

const fmt = (n) => {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};
const fS = (n) => {
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(0)}M`;
  return `${(n / 1e3).toFixed(0)}K`;
};

const P = { bg: "#060911", card: "#0c1020", border: "#151d38", subtle: "#1a2545", text: "#e4eaf3", muted: "#4f5e82" };

const selectStyle = {
  background: "#0f1530",
  color: "#e4eaf3",
  border: "1px solid #252d55",
  borderRadius: 6,
  padding: "8px 14px",
  fontSize: 13,
  fontWeight: 600,
  fontFamily: "'IBM Plex Mono', monospace",
  cursor: "pointer",
  outline: "none",
  appearance: "none",
  WebkitAppearance: "none",
  backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%234f5e82' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E\")",
  backgroundRepeat: "no-repeat",
  backgroundPosition: "right 10px center",
  paddingRight: 32,
};

export default function RevenueSimulator() {
  const { data: apiData, loading, error, refetch } = useApiData("/api/comparison");
  const [ratesDex, setRatesDex] = useState("km");
  const [volumeDex, setVolumeDex] = useState("xyz");

  const result = useMemo(() => {
    if (!apiData?.dex_summaries) return null;

    const rateData = apiData.dex_summaries.find((d) => d.dex === ratesDex);
    const volData = apiData.dex_summaries.find((d) => d.dex === volumeDex);
    if (!rateData || !volData) return null;

    const effBps = rateData.eff_deployer_bps || 0;
    const normalBps = effBps > 0 ? effBps / 0.10 : 0;
    const avg30d = volData.avg_30d || 0;
    const avg7d = volData.avg_7d || 0;

    const annGrowth30 = avg30d * 365 * effBps / 10000;
    const annNormal30 = avg30d * 365 * normalBps / 10000;
    const annGrowth7 = avg7d * 365 * effBps / 10000;
    const annNormal7 = avg7d * 365 * normalBps / 10000;

    return {
      rateData,
      volData,
      effBps,
      normalBps,
      avg30d,
      avg7d,
      annGrowth30,
      annNormal30,
      annGrowth7,
      annNormal7,
    };
  }, [apiData, ratesDex, volumeDex]);

  // Generate chart data: all combinations
  const allCombinations = useMemo(() => {
    if (!apiData?.dex_summaries) return [];
    const rateData = apiData.dex_summaries.find((d) => d.dex === ratesDex);
    if (!rateData) return [];
    const effBps = rateData.eff_deployer_bps || 0;
    const normalBps = effBps > 0 ? effBps / 0.10 : 0;

    return DEXES.map((volDex) => {
      const volData = apiData.dex_summaries.find((d) => d.dex === volDex);
      if (!volData) return null;
      const avg30d = volData.avg_30d || 0;
      return {
        name: DEX_META[volDex].name,
        color: DEX_META[volDex].color,
        growth: avg30d * 365 * effBps / 10000,
        normal: avg30d * 365 * normalBps / 10000,
        isSelected: volDex === volumeDex,
      };
    }).filter(Boolean).sort((a, b) => b.normal - a.normal);
  }, [apiData, ratesDex, volumeDex]);

  if (loading) return <Loading message="Loading simulator data..." />;
  if (error && !apiData) return <ErrorState error={error} onRetry={refetch} />;
  if (apiData?.status === "loading") return <Loading message={apiData.message} />;

  return (
    <div style={{ background: P.bg, color: P.text, minHeight: "100vh", fontFamily: "'IBM Plex Mono', monospace", padding: "20px 24px" }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 22, fontWeight: 700, margin: 0 }}>Revenue Simulator</h1>
        <p style={{ color: P.muted, fontSize: 11, margin: "4px 0 0" }}>
          Explore implied revenue across different rate × volume combinations
        </p>
      </div>

      {/* Selector */}
      <div style={{
        background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: "24px 28px", marginBottom: 20,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
          <span style={{ fontSize: 14, fontWeight: 600, fontFamily: "'IBM Plex Sans'", color: P.muted }}>If</span>

          <div style={{ position: "relative" }}>
            <select
              value={ratesDex}
              onChange={(e) => setRatesDex(e.target.value)}
              style={{ ...selectStyle, borderColor: DEX_META[ratesDex].color + "60", color: DEX_META[ratesDex].color }}
            >
              {DEXES.map((dx) => (
                <option key={dx} value={dx}>{DEX_META[dx].name}</option>
              ))}
            </select>
          </div>

          <span style={{ fontSize: 14, fontWeight: 600, fontFamily: "'IBM Plex Sans'", color: P.muted }}>had the volume of</span>

          <div style={{ position: "relative" }}>
            <select
              value={volumeDex}
              onChange={(e) => setVolumeDex(e.target.value)}
              style={{ ...selectStyle, borderColor: DEX_META[volumeDex].color + "60", color: DEX_META[volumeDex].color }}
            >
              {DEXES.map((dx) => (
                <option key={dx} value={dx}>{DEX_META[dx].name}</option>
              ))}
            </select>
          </div>
        </div>

        {/* Result */}
        {result && result.effBps > 0 && (
          <div style={{ marginTop: 20, display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14 }}>
            <div style={{ background: P.bg, borderRadius: 8, padding: "14px 16px" }}>
              <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 4 }}>Fee Rate (from {DEX_META[ratesDex].name})</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: DEX_META[ratesDex].color }}>
                {result.effBps.toFixed(4)} bps
              </div>
              <div style={{ fontSize: 10, color: P.muted, marginTop: 2 }}>{result.normalBps.toFixed(2)} bps normal</div>
            </div>
            <div style={{ background: P.bg, borderRadius: 8, padding: "14px 16px" }}>
              <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 4 }}>Volume (from {DEX_META[volumeDex].name})</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: DEX_META[volumeDex].color }}>
                {fmt(result.avg30d)}/day
              </div>
              <div style={{ fontSize: 10, color: P.muted, marginTop: 2 }}>30d avg · {fmt(result.avg7d)}/d 7d avg</div>
            </div>
            <div style={{ background: P.bg, borderRadius: 8, padding: "14px 16px" }}>
              <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 4 }}>Growth Mode (ann.)</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: "#00e5a0" }}>{fmt(result.annGrowth30)}/yr</div>
              <div style={{ fontSize: 10, color: P.muted, marginTop: 2 }}>{fmt(result.annGrowth30 / 12)}/mo</div>
            </div>
            <div style={{ background: P.bg, borderRadius: 8, padding: "14px 16px" }}>
              <div style={{ color: P.muted, fontSize: 9, textTransform: "uppercase", fontWeight: 600, marginBottom: 4 }}>Normal Mode (ann.)</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: "#7c5cfc" }}>{fmt(result.annNormal30)}/yr</div>
              <div style={{ fontSize: 10, color: P.muted, marginTop: 2 }}>{fmt(result.annNormal30 / 12)}/mo</div>
            </div>
          </div>
        )}

        {result && result.effBps === 0 && (
          <div style={{ marginTop: 20, padding: "16px", background: P.bg, borderRadius: 8, color: P.muted, fontSize: 12, textAlign: "center" }}>
            No effective fee rate available for {DEX_META[ratesDex].name}. Cannot compute implied revenue.
          </div>
        )}
      </div>

      {/* Chart: all volume sources with selected rates */}
      <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20, marginBottom: 20 }}>
        <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 4px", fontWeight: 600 }}>
          {DEX_META[ratesDex].name} rates × all volume sources
        </h3>
        <p style={{ color: P.muted, fontSize: 10, margin: "0 0 16px" }}>
          Annualized revenue at {result?.effBps?.toFixed(4) || "?"} bps growth / {result?.normalBps?.toFixed(2) || "?"} bps normal
        </p>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={allCombinations} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke={P.subtle} opacity={0.3} />
            <XAxis dataKey="name" tick={{ fill: P.text, fontSize: 11 }} />
            <YAxis tick={{ fill: P.muted, fontSize: 9 }} tickFormatter={fS} />
            <Tooltip formatter={(v) => fmt(v)} />
            <Legend wrapperStyle={{ fontSize: 10 }} />
            <Bar dataKey="growth" name="Growth Mode" fill="#00e5a0" opacity={0.9} radius={[3, 3, 0, 0]} />
            <Bar dataKey="normal" name="Normal Mode" fill="#6366f1" opacity={0.7} radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Full matrix table */}
      <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: 20 }}>
        <h3 style={{ fontFamily: "'IBM Plex Sans'", fontSize: 14, margin: "0 0 14px", fontWeight: 600 }}>Full Revenue Matrix (Normal Mode, Ann.)</h3>
        <p style={{ color: P.muted, fontSize: 10, margin: "-8px 0 14px" }}>Rows = fee rates from · Columns = volume from</p>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${P.border}` }}>
                <th style={{ padding: "8px", textAlign: "left", color: P.muted, fontSize: 9, fontWeight: 600 }}>RATES ↓ · VOL →</th>
                {DEXES.map((vd) => (
                  <th key={vd} style={{ padding: "8px", textAlign: "right", color: DEX_META[vd].color, fontSize: 10, fontWeight: 600 }}>
                    {DEX_META[vd].name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {DEXES.map((rd) => {
                const rateData = apiData?.dex_summaries?.find((d) => d.dex === rd);
                const effBps = rateData?.eff_deployer_bps || 0;
                const normalBps = effBps > 0 ? effBps / 0.10 : 0;

                return (
                  <tr key={rd} style={{ borderBottom: `1px solid ${P.subtle}` }}>
                    <td style={{ padding: "8px", fontWeight: 600 }}>
                      <span style={{ color: DEX_META[rd].color }}>●</span> {DEX_META[rd].name}
                      <span style={{ color: P.muted, fontSize: 9, marginLeft: 6 }}>
                        ({normalBps > 0 ? normalBps.toFixed(2) : "?"} bps)
                      </span>
                    </td>
                    {DEXES.map((vd) => {
                      const volData = apiData?.dex_summaries?.find((d) => d.dex === vd);
                      const avg30d = volData?.avg_30d || 0;
                      const rev = avg30d * 365 * normalBps / 10000;
                      const isSelected = rd === ratesDex && vd === volumeDex;

                      return (
                        <td
                          key={vd}
                          onClick={() => { setRatesDex(rd); setVolumeDex(vd); }}
                          style={{
                            padding: "8px",
                            textAlign: "right",
                            fontWeight: isSelected ? 700 : 400,
                            color: isSelected ? "#f472b6" : rev > 0 ? P.text : P.muted,
                            background: isSelected ? "#f472b610" : "transparent",
                            cursor: "pointer",
                            borderRadius: 4,
                            transition: "background 0.1s",
                          }}
                        >
                          {rev > 0 ? fmt(rev) : "—"}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div style={{ marginTop: 10, fontSize: 9, color: P.muted }}>
          Click any cell to select that combination
        </div>
      </div>

      <div style={{ marginTop: 14, fontSize: 9, color: P.subtle, textAlign: "center" }}>
        Hyperliquid L1 API · Auto-refresh every 5 min · {apiData?.generated_at}
      </div>
    </div>
  );
}
