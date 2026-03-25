const P = {
  bg: "#060911", card: "#0c1020", border: "#151d38",
  text: "#e4eaf3", muted: "#4f5e82", green: "#00e5a0",
};

export function Loading({ message = "Loading data..." }) {
  return (
    <div style={{ background: P.bg, color: P.text, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'IBM Plex Mono', monospace" }}>
      <div style={{ textAlign: "center" }}>
        <div style={{ width: 32, height: 32, border: `3px solid ${P.border}`, borderTopColor: P.green, borderRadius: "50%", animation: "spin 0.8s linear infinite", margin: "0 auto 16px" }} />
        <p style={{ color: P.muted, fontSize: 12 }}>{message}</p>
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      </div>
    </div>
  );
}

export function ErrorState({ error, onRetry }) {
  return (
    <div style={{ background: P.bg, color: P.text, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'IBM Plex Mono', monospace" }}>
      <div style={{ background: P.card, border: `1px solid ${P.border}`, borderRadius: 10, padding: "32px 40px", textAlign: "center", maxWidth: 400 }}>
        <div style={{ fontSize: 28, marginBottom: 12 }}>⚠</div>
        <p style={{ fontSize: 13, marginBottom: 8 }}>Failed to load data</p>
        <p style={{ color: P.muted, fontSize: 11, marginBottom: 16 }}>{error}</p>
        {onRetry && (
          <button onClick={onRetry} style={{ background: P.green, color: "#060911", border: "none", borderRadius: 6, padding: "8px 20px", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>Retry</button>
        )}
      </div>
    </div>
  );
}
