import { useState } from "react";
import RevenueDashboard from "./dashboards/Revenue";
import ComparisonDashboard from "./dashboards/Comparison";
import LiquidityDashboard from "./dashboards/Liquidity";
import RevenueSimulator from "./dashboards/RevenueSimulator";
import UsersDashboard from "./dashboards/Users";
import BuybacksDashboard from "./dashboards/Buybacks";
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
  {
    section: "Buybacks",
    icon: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <path d="M23 4v6h-6" /><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
      </svg>
    ),
    items: [{ id: "buybacks", label: "sKNTQ Buybacks", color: "#ffb020" }],
  },
  {
    section: "Users",
    icon: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
        <circle cx="9" cy="7" r="4" />
        <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
        <path d="M16 3.13a4 4 0 0 1 0 7.75" />
      </svg>
    ),
    items: [{ id: "users", label: "HIP-3 Users", color: "#a78bfa" }],
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
      case "users":      return <UsersDashboard />;
      case "buybacks":   return <BuybacksDashboard />;
      default: return <RevenueDashboard dexId="km" />;
    }
  };

  return (
    <div style={{ background: C.bg, minHeight: "100vh", display: "flex" }}>
      <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet" />
      <style>{`
        @media (max-width: 680px) {
          .app-sidebar { width: 52px !important; }
          .app-main { margin-left: 52px !important; width: calc(100% - 52px) !important; min-width: 0; flex: none !important; }
          .app-nav-label, .app-sidebar-footer { display: none !important; }
          .app-nav-button { padding: 7px 16px !important; }
          .app-logo { padding: 16px 14px !important; }
        }
      `}</style>

      {/* Sidebar */}
      <aside className="app-sidebar" style={{
        width: collapsed ? 52 : 210, minHeight: "100vh", background: C.sidebar,
        borderRight: `1px solid ${C.border}`, display: "flex", flexDirection: "column",
        transition: "width 0.2s ease", position: "fixed", top: 0, left: 0, bottom: 0, zIndex: 100, overflow: "hidden",
      }}>
        {/* Logo */}
        <div className="app-logo"
          style={{ padding: collapsed ? "16px 14px" : "16px 18px", borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", gap: 10, cursor: "pointer", minHeight: 52 }}
          onClick={() => setCollapsed(!collapsed)}
        >
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: C.green, boxShadow: `0 0 10px ${C.green}`, flexShrink: 0 }} />
          {!collapsed && <span className="app-nav-label" style={{ fontFamily: "'IBM Plex Sans'", fontWeight: 700, fontSize: 15, color: C.text, whiteSpace: "nowrap" }}>HIP-3</span>}
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
                {!collapsed && <span className="app-nav-label">{section.section}</span>}
              </div>
              {section.items.map((item) => {
                const active = page === item.id;
                return (
                  <button className="app-nav-button" key={item.id} onClick={() => setPage(item.id)} style={{
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
                    {!collapsed && <span className="app-nav-label">{item.label}</span>}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>

        {!collapsed && (
          <div className="app-sidebar-footer" style={{ padding: "12px 18px", borderTop: `1px solid ${C.border}`, fontSize: 9, color: C.dimmed, fontFamily: "'IBM Plex Mono', monospace", lineHeight: 1.6 }}>
            100% on-chain<br />Hyperliquid L1
          </div>
        )}
      </aside>

      {/* Main */}
      <main className="app-main" style={{ flex: 1, marginLeft: collapsed ? 52 : 210, transition: "margin-left 0.2s ease", minHeight: "100vh" }}>
        {renderPage()}
      </main>
    </div>
  );
}
