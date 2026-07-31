import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  View, Text, StyleSheet, ScrollView, TouchableOpacity,
  SafeAreaView, RefreshControl, StatusBar, Alert, TextInput, Switch,
} from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { DEFAULT_API_HOST, DEFAULT_API_TOKEN, buildUrls } from './config';

const STORAGE_KEY = 'rubaih.greeks.connection.v1';

const C = {
  bg: '#0b1220',
  card: '#141e2e',
  border: '#243247',
  text: '#eef3f8',
  muted: '#8fa0b3',
  accent: '#3dd6c6',
  accentDim: 'rgba(61,214,198,0.14)',
  good: '#1db954',
  bad: '#e74c3c',
  inputBg: '#0f1724',
};

const TABS = [
  { id: 'dashboard', label: 'Home' },
  { id: 'trades', label: 'Trades' },
  { id: 'settings', label: 'Setup' },
];

function fmt(n, d = 2) {
  if (n == null || Number.isNaN(Number(n))) return '—';
  return Number(n).toFixed(d);
}

export default function App() {
  const [ready, setReady] = useState(false);
  const [hostInput, setHostInput] = useState(DEFAULT_API_HOST);
  const [tokenInput, setTokenInput] = useState(DEFAULT_API_TOKEN);
  const [conn, setConn] = useState(() => buildUrls(DEFAULT_API_HOST, DEFAULT_API_TOKEN));
  const [dashboard, setDashboard] = useState(null);
  const [settings, setSettings] = useState(null);
  const [trades, setTrades] = useState([]);
  const [connected, setConnected] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [activeTab, setActiveTab] = useState('settings');
  const [saving, setSaving] = useState(false);
  const wsRef = useRef(null);
  const connRef = useRef(conn);
  const aliveRef = useRef(true);

  useEffect(() => { connRef.current = conn; }, [conn]);

  useEffect(() => {
    (async () => {
      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEY);
        if (raw) {
          const saved = JSON.parse(raw);
          const next = buildUrls(saved.host || DEFAULT_API_HOST, saved.token || DEFAULT_API_TOKEN);
          setHostInput(next.apiHost || DEFAULT_API_HOST);
          setTokenInput(next.apiToken || DEFAULT_API_TOKEN);
          setConn(next);
          if (next.configured) setActiveTab('dashboard');
        } else if (buildUrls(DEFAULT_API_HOST, DEFAULT_API_TOKEN).configured) {
          setActiveTab('dashboard');
        }
      } finally {
        setReady(true);
      }
    })();
    return () => { aliveRef.current = false; };
  }, []);

  const apiFetch = useCallback(async (path, opts = {}) => {
    const c = connRef.current;
    const res = await fetch(`${c.apiUrl}${path}`, {
      ...opts,
      headers: { ...c.authHeaders, ...(opts.headers || {}) },
    });
    if (!res.ok) throw new Error(`${path} → ${res.status}`);
    return res.json();
  }, []);

  const refresh = useCallback(async () => {
    const c = connRef.current;
    if (!c.configured) return;
    setRefreshing(true);
    try {
      const [dash, set, tr] = await Promise.all([
        apiFetch('/dashboard'),
        apiFetch('/settings'),
        apiFetch('/trades?limit=30'),
      ]);
      if (!aliveRef.current) return;
      setDashboard(dash);
      setSettings(set);
      setTrades(Array.isArray(tr) ? tr : []);
      setConnected(true);
    } catch (e) {
      setConnected(false);
      console.warn(e);
    } finally {
      setRefreshing(false);
    }
  }, [apiFetch]);

  useEffect(() => {
    if (!ready || !conn.configured) return;
    refresh();
    const id = setInterval(refresh, 8000);
    return () => clearInterval(id);
  }, [ready, conn.configured, conn.apiHost, conn.apiToken, refresh]);

  useEffect(() => {
    if (!ready || !conn.configured) return;
    if (wsRef.current) {
      try { wsRef.current.close(); } catch (_) {}
    }
    let ws;
    try {
      ws = new WebSocket(conn.wsUrl);
      wsRef.current = ws;
      ws.onopen = () => setConnected(true);
      ws.onclose = () => setConnected(false);
      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          setDashboard((prev) => ({ ...(prev || {}), ...data }));
        } catch (_) {}
      };
    } catch (e) {
      console.warn(e);
    }
    return () => {
      try { ws && ws.close(); } catch (_) {}
    };
  }, [ready, conn.wsUrl, conn.configured]);

  const saveConnection = async () => {
    setSaving(true);
    try {
      const next = buildUrls(hostInput, tokenInput);
      await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify({ host: next.apiHost, token: next.apiToken }));
      setConn(next);
      if (next.configured) {
        setActiveTab('dashboard');
        Alert.alert('Saved', 'Connected to Rubaih Greeks');
      } else {
        Alert.alert('Incomplete', 'Set host and Greeks API token');
      }
    } finally {
      setSaving(false);
    }
  };

  const kill = () => {
    Alert.alert('Kill switch', 'Flatten and stop the Greeks engine?', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'Kill',
        style: 'destructive',
        onPress: async () => {
          try {
            await apiFetch('/kill', { method: 'POST' });
            Alert.alert('Queued', 'Kill command sent');
            refresh();
          } catch (e) {
            Alert.alert('Failed', String(e.message || e));
          }
        },
      },
    ]);
  };

  if (!ready) {
    return (
      <SafeAreaView style={[styles.root, { justifyContent: 'center', alignItems: 'center' }]}>
        <Text style={{ color: C.muted }}>Loading…</Text>
      </SafeAreaView>
    );
  }

  const pos = dashboard?.position;
  const live = String(dashboard?.live_trading ?? settings?.live_trading ?? 'false') === 'true';

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" />
      <View style={styles.header}>
        <View>
          <Text style={styles.brand}>Rubaih Greeks</Text>
          <Text style={styles.sub}>Delta options · {connected ? 'online' : 'offline'}</Text>
        </View>
        <View style={[styles.pill, { backgroundColor: live ? 'rgba(231,76,60,0.2)' : C.accentDim }]}>
          <Text style={{ color: live ? C.bad : C.accent, fontWeight: '700', fontSize: 12 }}>
            {live ? 'LIVE' : 'DRY-RUN'}
          </Text>
        </View>
      </View>

      <ScrollView
        style={{ flex: 1 }}
        contentContainerStyle={{ padding: 16, paddingBottom: 100 }}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={refresh} tintColor={C.accent} />}
      >
        {activeTab === 'dashboard' && (
          <>
            <View style={styles.card}>
              <Text style={styles.cardTitle}>Capital</Text>
              <Row label="Free (quote)" value={fmt(dashboard?.free_capital_inr ?? settings?.free_capital_inr)} />
              <Row label="Budget" value={fmt(dashboard?.budget_inr)} />
              <Row label="Session PnL" value={fmt(dashboard?.session_pnl)} />
              <Row label="Engine" value={String(dashboard?.engine_status || '—')} />
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>Position</Text>
              {!pos ? (
                <Text style={styles.help}>Flat — scanning BTC/ETH options</Text>
              ) : (
                <>
                  <Row label="Symbol" value={pos.symbol} />
                  <Row label="Type" value={`${pos.option_type} · K ${fmt(pos.strike, 0)}`} />
                  <Row label="Size" value={String(pos.size)} />
                  <Row label="Entry" value={fmt(pos.entry, 4)} />
                  <Row label="Mark" value={fmt(pos.mark, 4)} />
                  <Row label="uPnL" value={fmt(pos.upnl)} />
                  <Row label="TP / SL" value={`${fmt(pos.tp, 4)} / ${fmt(pos.sl, 4)}`} />
                </>
              )}
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>Exits</Text>
              <Row label="Take profit" value={settings?.tp_display || 'Premium +25%'} />
              <Row label="Stop loss" value={settings?.sl_display || 'Premium −12%'} />
              <Row label="Underlyings" value={settings?.underlyings || 'BTC,ETH'} />
              <Text style={styles.help}>
                Day-1: keep dry-run. Quant owns entries/exits; AI is advisory only.
              </Text>
            </View>

            <TouchableOpacity style={styles.dangerBtn} onPress={kill}>
              <Text style={styles.dangerText}>Kill switch</Text>
            </TouchableOpacity>
          </>
        )}

        {activeTab === 'trades' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Recent trades</Text>
            {trades.length === 0 ? (
              <Text style={styles.help}>No trades yet</Text>
            ) : (
              trades.map((t, i) => (
                <View key={`${t.id || i}`} style={styles.tradeRow}>
                  <Text style={styles.tradeMain}>
                    {(t.side || '').toUpperCase()} {t.symbol || '—'}
                  </Text>
                  <Text style={styles.help}>
                    {t.option_type || ''} · size {fmt(t.size, 0)} · prem {fmt(t.premium, 4)}
                  </Text>
                  <Text style={styles.help}>{t.reason || ''}</Text>
                </View>
              ))
            )}
          </View>
        )}

        {activeTab === 'settings' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Connection</Text>
            <Text style={styles.label}>API host (nginx :8088)</Text>
            <TextInput
              style={styles.input}
              value={hostInput}
              onChangeText={setHostInput}
              autoCapitalize="none"
              autoCorrect={false}
              placeholderTextColor={C.muted}
            />
            <Text style={styles.label}>Greeks API token</Text>
            <TextInput
              style={styles.input}
              value={tokenInput}
              onChangeText={setTokenInput}
              autoCapitalize="none"
              autoCorrect={false}
              secureTextEntry
              placeholderTextColor={C.muted}
            />
            <TouchableOpacity style={styles.primaryBtn} onPress={saveConnection} disabled={saving}>
              <Text style={styles.primaryText}>{saving ? 'Saving…' : 'Save & connect'}</Text>
            </TouchableOpacity>
            <Text style={styles.help}>
              Same VPS as futures Rubaih. Greeks uses port 8088; futures uses 8080.
            </Text>
          </View>
        )}
      </ScrollView>

      <View style={styles.tabs}>
        {TABS.map((t) => (
          <TouchableOpacity key={t.id} style={styles.tab} onPress={() => setActiveTab(t.id)}>
            <Text style={[styles.tabText, activeTab === t.id && styles.tabActive]}>{t.label}</Text>
          </TouchableOpacity>
        ))}
      </View>
    </SafeAreaView>
  );
}

function Row({ label, value }) {
  return (
    <View style={styles.row}>
      <Text style={styles.rowLabel}>{label}</Text>
      <Text style={styles.rowValue}>{value}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: C.bg },
  header: {
    paddingHorizontal: 16, paddingTop: 8, paddingBottom: 12,
    flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
    borderBottomWidth: 1, borderBottomColor: C.border,
  },
  brand: { color: C.text, fontSize: 22, fontWeight: '800', letterSpacing: 0.3 },
  sub: { color: C.muted, marginTop: 2, fontSize: 12 },
  pill: { paddingHorizontal: 10, paddingVertical: 6, borderRadius: 999 },
  card: {
    backgroundColor: C.card, borderColor: C.border, borderWidth: 1,
    borderRadius: 14, padding: 14, marginBottom: 12,
  },
  cardTitle: { color: C.text, fontSize: 15, fontWeight: '700', marginBottom: 10 },
  help: { color: C.muted, fontSize: 12, lineHeight: 18, marginTop: 8 },
  row: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 8 },
  rowLabel: { color: C.muted, fontSize: 13 },
  rowValue: { color: C.text, fontSize: 13, fontWeight: '600', maxWidth: '62%', textAlign: 'right' },
  label: { color: C.muted, fontSize: 12, marginBottom: 6, marginTop: 8 },
  input: {
    backgroundColor: C.inputBg, borderColor: C.border, borderWidth: 1, borderRadius: 10,
    color: C.text, paddingHorizontal: 12, paddingVertical: 10, marginBottom: 4,
  },
  primaryBtn: {
    marginTop: 14, backgroundColor: C.accent, borderRadius: 12, paddingVertical: 12, alignItems: 'center',
  },
  primaryText: { color: '#04221f', fontWeight: '800' },
  dangerBtn: {
    backgroundColor: 'rgba(231,76,60,0.15)', borderColor: C.bad, borderWidth: 1,
    borderRadius: 12, paddingVertical: 14, alignItems: 'center', marginTop: 4,
  },
  dangerText: { color: C.bad, fontWeight: '800' },
  tradeRow: { paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: C.border },
  tradeMain: { color: C.text, fontWeight: '700', marginBottom: 2 },
  tabs: {
    position: 'absolute', left: 0, right: 0, bottom: 0,
    flexDirection: 'row', backgroundColor: C.card, borderTopWidth: 1, borderTopColor: C.border,
    paddingBottom: 10, paddingTop: 8,
  },
  tab: { flex: 1, alignItems: 'center', paddingVertical: 8 },
  tabText: { color: C.muted, fontWeight: '600' },
  tabActive: { color: C.accent },
});
