import { useState, useEffect, useCallback } from "react";

function getApiBase() {
  const configured = (import.meta.env.VITE_API_URL ?? "").trim();
  if (!configured || configured.includes("TU_DOMINIO")) return "";

  if (typeof window !== "undefined" && window.location.protocol === "https:") {
    if (configured.startsWith("http://")) return "";

    try {
      const apiUrl = new URL(configured);
      const isIpHost = /^\d{1,3}(\.\d{1,3}){3}$/.test(apiUrl.hostname);
      if (apiUrl.protocol === "https:" && apiUrl.port && isIpHost) return "";
    } catch {
      return "";
    }
  }

  return configured.replace(/\/+$/, "");
}

const API_BASE = getApiBase();
const REFRESH_INTERVAL = 5 * 60 * 1000;

export function apiUrl(endpoint) {
  if (!endpoint) return null;
  if (/^https?:\/\//i.test(endpoint)) return endpoint;
  const path = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
  return `${API_BASE}${path}`;
}

export function useApiData(endpoint) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async () => {
    const url = apiUrl(endpoint);
    if (!url) {
      setData(null);
      setError(null);
      setLoading(false);
      return;
    }

    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (err) {
      console.error(`Failed to fetch ${endpoint}:`, err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [endpoint]);

  useEffect(() => {
    fetchData();
    if (!endpoint) return undefined;
    const interval = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
