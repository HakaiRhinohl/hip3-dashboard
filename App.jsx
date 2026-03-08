import { useState } from "react";
import RevenueDashboard from "./dashboards/Revenue";
import ComparisonDashboard from "./dashboards/Comparison";
import LiquidityDashboard from "./dashboards/Liquidity";

const PAGES = [
  { id: "revenue", label: "Revenue", icon: "◈" },
  { id: "comparison", label: "HIP-3 Comparison", icon: "◇" },
  { id: "liquidity", label: "Liquidity", icon: "◆" },
];

const C = {
  bg: "#060911",
  nav: "#0a0e1a",
  border: "#151d38",
  green: "#00e5a0",
  text: "#e4eaf3",
  muted: "#4f5e82",
};

export default function App() {
  const [page, setPage] = useState("revenue");

  return (
    <div style={{ background: C.bg, minHeight: "100vh" }}>
      <link
        href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap"
        rel="stylesheet"
      />

      {/* Top navigation bar */}
      <nav
        style={{
          background: C.nav,
          borderBottom: `1px solid ${C.border}`,
          padding: "0 24px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          height: 48,
          position: "sticky",
          top: 0,
          zIndex: 100,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
          {/* Logo */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontFamily: "'IBM Plex Sans'",
              fontWeight: 700,
              fontSize: 14,
              color: C.text,
              cursor: "pointer",
            }}
            onClick={() => setPage("revenue")}
          >
            <div
              style={{
                width: 7,
                height: 7,
                borderRadius: "50%",
                background: C.green,
                boxShadow: `0 0 10px ${C.green}`,
              }}
            />
            Kinetiq
          </div>

          {/* Nav links */}
          <div style={{ display: "flex", gap: 2 }}>
            {PAGES.map((p) => (
              <button
                key={p.id}
                onClick={() => setPage(p.id)}
                style={{
                  background: page === p.id ? "#111a30" : "transparent",
                  color: page === p.id ? C.green : C.muted,
                  border: "none",
                  borderRadius: 4,
                  padding: "6px 14px",
                  fontSize: 11,
                  fontWeight: 600,
                  cursor: "pointer",
                  fontFamily: "'IBM Plex Mono', monospace",
                  transition: "all 0.15s",
                }}
              >
                <span style={{ marginRight: 4 }}>{p.icon}</span>
                {p.label}
              </button>
            ))}
          </div>
        </div>

        <div
          style={{
            fontSize: 10,
            color: C.muted,
            fontFamily: "'IBM Plex Mono', monospace",
          }}
        >
          100% on-chain · Hyperliquid L1
        </div>
      </nav>

      {/* Page content */}
      {page === "revenue" && <RevenueDashboard />}
      {page === "comparison" && <ComparisonDashboard />}
      {page === "liquidity" && <LiquidityDashboard />}
    </div>
  );
}
