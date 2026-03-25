import { useState } from "react";
import RevenueDashboard from "./dashboards/Revenue";
import ComparisonDashboard from "./dashboards/Comparison";
import LiquidityDashboard from "./dashboards/Liquidity";
import RevenueSimulator from "./dashboards/RevenueSimulator";
const DEX_LIST = [
  { id: "km", label: "Markets", color: "#00e5a0" },
  { id: "xyz", label: "Trade.xyz", color: "#7c5cfc" },
  { id: "flx", label: "Felix", color: "#ff4d6a" },
  { id: "cash", label: "Dreamcash", color: "#ffb020" },
];

const NAV = [
  {
    section: "Revenue",
    icon: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
      </svg>
    ),
    items: DEX_LIST.map((d) => ({ id: `revenue-${d.id}`, label: d.label, color: d.color })),
  },
  {
    section: "Comparison",
    icon: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
        <rect x="3" y="14" width="7" height="7" /><rect x="14" y="14" width="7" height="7" />
      </svg>
    ),
    items: [
      { id: "comparison", label: "Overview", color: "#38bdf8" },
      { id: "simulator",  label: "Revenue Simulator", color: "#f472b6" },
    ],
  },
  {
    section: "Liquidity",
    icon: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <path d="M12 2.69l5.66 5.66a8 8 0 1 1-11.31 0z" />
      </svg>
    ),
    items: [{ id: "liquidity", label: "Orderbook Analysis", color: "#2dd4bf" }],
  },
];

const C = {
  bg: "#060911",
  sidebar: "#0a0e1a",
  sidebarHover: "#0f1528",
  border: "#141c34",
  green: "#00e5a0",
  text: "#e4eaf3",
  muted: "#4a5578",
  dimmed: "#2a3352",
};

export default function App() {
  const [page, setPage] = useState("revenue-km");
  const [collapsed, setCollapsed] = useState(false);

  const renderPage = () => {
    if (page.startsWith("revenue-")) {
      const dexId = page.replace("revenue-", "");
      return <RevenueDashboard dexId={dexId} key={dexId} />;
    }
    switch (page) {
      case "comparison": return <ComparisonDashboard />;
      case "simulator":  return <RevenueSimulator />;
      case "liquidity":  return <LiquidityDashboard />;
      default: return <RevenueDashboard dexId="km" />;
    }
  };

  return (
    <div style={{ background: C.bg, minHeight: "100vh", display: "flex" }}>
      <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet" />

      {/* Sidebar */}
      <aside style={{
        width: collapsed ? 52 : 210, minHeight: "100vh", background: C.sidebar,
        borderRight: `1px solid ${C.border}`, display: "flex", flexDirection: "column",
        transition: "width 0.2s ease", position: "fixed", top: 0, left: 0, bottom: 0, zIndex: 100, overflow: "hidden",
      }}>
        {/* Logo */}
        <div
          style={{ padding: collapsed ? "16px 14px" : "16px 18px", borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", gap: 10, cursor: "pointer", minHeight: 52 }}
          onClick={() => setCollapsed(!collapsed)}
        >
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: C.green, boxShadow: `0 0 10px ${C.green}`, flexShrink: 0 }} />
          {!collapsed && <span style={{ fontFamily: "'IBM Plex Sans'", fontWeight: 700, fontSize: 15, color: C.text, whiteSpace: "nowrap" }}>HIP-3</span>}
        </div>

        {/* Nav */}
        <nav style={{ flex: 1, padding: "12px 0", overflowY: "auto" }}>
          {NAV.map((section) => (
            <div key={section.section} style={{ marginBottom: 8 }}>
              <div style={{
                padding: collapsed ? "6px 16px" : "6px 18px", display: "flex", alignItems: "center", gap: 8,
                color: C.dimmed, fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em",
                fontFamily: "'IBM Plex Mono', monospace", whiteSpace: "nowrap",
              }}>
                <span style={{ flexShrink: 0, display: "flex" }}>{section.icon}</span>
                {!collapsed && section.section}
              </div>
              {section.items.map((item) => {
                const active = page === item.id;
                return (
                  <button key={item.id} onClick={() => setPage(item.id)} style={{
                    display: "flex", alignItems: "center", gap: 10, width: "100%",
                    padding: collapsed ? "7px 16px" : "7px 18px 7px 28px",
                    background: active ? `${item.color}10` : "transparent",
                    border: "none", borderLeft: active ? `2px solid ${item.color}` : "2px solid transparent",
                    color: active ? item.color : C.muted, fontSize: 11, fontWeight: active ? 600 : 400,
                    fontFamily: "'IBM Plex Mono', monospace", cursor: "pointer", textAlign: "left",
                    whiteSpace: "nowrap", transition: "all 0.12s ease",
                  }}
                    onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = C.sidebarHover; }}
                    onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}
                  >
                    <div style={{ width: collapsed ? 6 : 4, height: collapsed ? 6 : 4, borderRadius: "50%", background: active ? item.color : C.dimmed, flexShrink: 0 }} />
                    {!collapsed && item.label}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>

        {!collapsed && (
          <div style={{ padding: "12px 18px", borderTop: `1px solid ${C.border}`, fontSize: 9, color: C.dimmed, fontFamily: "'IBM Plex Mono', monospace", lineHeight: 1.6 }}>
            100% on-chain<br />Hyperliquid L1
          </div>
        )}
      </aside>

      {/* Main */}
      <main style={{ flex: 1, marginLeft: collapsed ? 52 : 210, transition: "margin-left 0.2s ease", minHeight: "100vh" }}>
        {renderPage()}
      </main>
    </div>
  );
}
