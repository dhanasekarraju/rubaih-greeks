import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  View, Text, StyleSheet, ScrollView, TouchableOpacity,
  SafeAreaView, RefreshControl, StatusBar, Alert, TextInput,
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
  info: '#5b9fd4',
  inputBg: '#0f1724',
  logBg: '#080e18',
};

const TABS = [
  { id: 'dashboard', label: 'Home' },
  { id: 'logs', label: 'Logs' },
  { id: 'signals', label: 'Signals' },
  { id: 'trades', label: 'Trades' },
  { id: 'settings', label: 'Setup' },
];

function fmt(n, d = 2) {
  if (n == null || Number.isNaN(Number(n))) return '—';
  return Number(n).toFixed(d);
}

function confPct(n) {
  if (n == null || Number.isNaN(Number(n))) return '—';
  return `${(Number(n) * 100).toFixed(0)}%`;
}

export default function App() {
  const [ready, setReady] = useState(false);
  const [hostInput, setHostInput] = useState(DEFAULT_API_HOST);
  const [tokenInput, setTokenInput] = useState(DEFAULT_API_TOKEN);
  const [conn, setConn] = useState(() => buildUrls(DEFAULT_API_HOST, DEFAULT_API_TOKEN));
  const [dashboard, setDashboard] = useState(null);
  const [settings, setSettings] = useState(null);
  const [balance, setBalance] = useState(null);
  const [trades, setTrades] = useState([]);
  const [logs, setLogs] = useState([]);
  const [signals, setSignals] = useState([]);
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
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 12000);
    try {
      const res = await fetch(`${c.apiUrl}${path}`, {
        ...opts,
        signal: ctrl.signal,
        headers: { ...c.authHeaders, ...(opts.headers || {}) },
      });
      if (!res.ok) throw new Error(`${path} → ${res.status}`);
      return res.json();
    } finally {
      clearTimeout(timer);
    }
  }, []);

  const refresh = useCallback(async () => {
    const c = connRef.current;
    if (!c.configured) return;
    setRefreshing(true);
    try {
      // Independent fetches — one 502/404 must not blank the whole Home screen
      const results = await Promise.allSettled([
        apiFetch('/dashboard'),
        apiFetch('/settings'),
        apiFetch('/trades?limit=30'),
        apiFetch('/balance'),
        apiFetch('/logs?limit=100'),
        apiFetch('/signals?limit=40'),
      ]);
      if (!aliveRef.current) return;
      const [dash, set, tr, bal, lg, sig] = results;
      let ok = 0;
      if (dash.status === 'fulfilled') { setDashboard(dash.value); ok += 1; }
      if (set.status === 'fulfilled') { setSettings(set.value); ok += 1; }
      if (tr.status === 'fulfilled') setTrades(Array.isArray(tr.value) ? tr.value : []);
      if (bal.status === 'fulfilled') setBalance(bal.value);
      if (lg.status === 'fulfilled') setLogs(Array.isArray(lg.value) ? lg.value : []);
      if (sig.status === 'fulfilled') setSignals(Array.isArray(sig.value) ? sig.value : []);
      setConnected(ok > 0);
      if (ok === 0) {
        const first = results.find((r) => r.status === 'rejected');
        console.warn(first?.reason || 'all endpoints failed');
      }
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
          if (data.channel === 'greeks:log' && data.data?.line) {
            setLogs((prev) => [{ ts: data.data.ts, line: data.data.line }, ...prev].slice(0, 120));
            return;
          }
          if (data.channel === 'greeks:signal' && data.data) {
            setSignals((prev) => [data.data, ...prev].slice(0, 50));
            setDashboard((prev) => ({
              ...(prev || {}),
              ai_last_action: data.data.action,
              ai_confidence: data.data.confidence,
            }));
            return;
          }
          if (!data.channel) {
            setDashboard((prev) => ({ ...(prev || {}), ...data }));
          }
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

  const refreshWallet = async () => {
    try {
      await apiFetch('/refresh-capital', { method: 'POST' });
      Alert.alert('Queued', 'Fetching live Delta wallet…');
      setTimeout(refresh, 2500);
    } catch (e) {
      const msg = String(e.message || e);
      Alert.alert(
        'Refresh failed',
        msg.includes('502')
          ? 'Nginx 502 — API down. On VPS run:\ndocker compose up -d --force-recreate greeks_api nginx'
          : msg,
      );
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

  const resumeHalt = () => {
    Alert.alert('Resume trading', 'Clear risk halt and allow new entries? Baseline drawdown resets.', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'Resume',
        onPress: async () => {
          try {
            await apiFetch('/resume', { method: 'POST' });
            Alert.alert('Queued', 'Resume sent — wait a few seconds');
            setTimeout(refresh, 2000);
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
  const live = String(dashboard?.live_trading ?? settings?.live_trading ?? 'false') === 'true'
    || dashboard?.live === true;
  const halted = dashboard?.halted === true || balance?.halted === true
    || String(dashboard?.engine_status || '').toLowerCase() === 'halted';
  const free = balance?.free_quote ?? balance?.free_capital ?? dashboard?.free_quote
    ?? dashboard?.free_capital_inr ?? settings?.free_capital_inr;
  const quoteCcy = balance?.quote_ccy || dashboard?.quote_ccy || settings?.quote_ccy || 'USDT';
  const freeInr = balance?.free_inr_approx ?? dashboard?.free_inr_approx
    ?? settings?.free_inr_approx;
  const source = balance?.source || dashboard?.capital_source || settings?.capital_source || '—';
  const walletRows = Array.isArray(balance?.balances) ? balance.balances : [];
  const freeLabel = free == null
    ? '—'
    : `${fmt(free, 4)} ${quoteCcy}${freeInr != null && Number(freeInr) > 0 ? `  (≈ ₹${fmt(freeInr, 0)})` : ''}`;
  const budgetRaw = dashboard?.budget_quote ?? dashboard?.budget_inr;
  const budgetLabel = budgetRaw == null ? '—' : `${fmt(budgetRaw, 4)} ${quoteCcy}`;
  const ddPct = dashboard?.drawdown_pct != null
    ? `${(Number(dashboard.drawdown_pct) * 100).toFixed(1)}%`
    : '—';

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" />
      <View style={styles.header}>
        <View>
          <Text style={styles.brand}>Rubaih Greeks</Text>
          <Text style={styles.sub}>Delta options · {connected ? 'online' : 'offline'}</Text>
        </View>
        <View style={{ flexDirection: 'row', alignItems: 'center', gap: 8 }}>
          {halted && (
            <View style={[styles.pill, { backgroundColor: 'rgba(231,76,60,0.25)' }]}>
              <Text style={{ color: C.bad, fontWeight: '700', fontSize: 12 }}>HALTED</Text>
            </View>
          )}
          <View style={[styles.pill, { backgroundColor: live ? 'rgba(231,76,60,0.2)' : C.accentDim }]}>
            <Text style={{ color: live ? C.bad : C.accent, fontWeight: '700', fontSize: 12 }}>
              {live ? 'LIVE' : 'DRY-RUN'}
            </Text>
          </View>
        </View>
      </View>

      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        style={styles.tabScroll}
        contentContainerStyle={styles.tabRow}
      >
        {TABS.map((t) => (
          <TouchableOpacity
            key={t.id}
            style={[styles.tabChip, activeTab === t.id && styles.tabChipOn]}
            onPress={() => setActiveTab(t.id)}
          >
            <Text style={[styles.tabChipText, activeTab === t.id && styles.tabChipTextOn]}>{t.label}</Text>
          </TouchableOpacity>
        ))}
      </ScrollView>

      <ScrollView
        style={{ flex: 1 }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={refresh} tintColor={C.accent} />}
      >
        {activeTab === 'dashboard' && (
          <>
            {!connected && (
              <View style={[styles.card, { borderColor: C.bad }]}>
                <Text style={styles.cardTitle}>API offline (502?)</Text>
                <Text style={styles.help}>
                  Host must be :8088. On VPS: docker compose ps && docker compose up -d --force-recreate greeks_api nginx
                </Text>
              </View>
            )}
            {halted && (
              <View style={[styles.card, { borderColor: C.bad }]}>
                <Text style={styles.cardTitle}>Risk halt</Text>
                <Text style={styles.help}>
                  {String(dashboard?.halt_reason || 'Drawdown / daily loss limit hit. No new entries until you resume.')}
                </Text>
                <TouchableOpacity style={styles.primaryBtn} onPress={resumeHalt} activeOpacity={0.8}>
                  <Text style={styles.primaryText} numberOfLines={1}>Resume trading</Text>
                </TouchableOpacity>
              </View>
            )}
            <View style={styles.card}>
              <Text style={styles.cardTitle}>Live account balance</Text>
              <Row label="Free (Delta)" value={freeLabel} />
              <Row label="Budget / trade" value={budgetLabel} />
              <Row label="Drawdown" value={ddPct} />
              <Row label="Source" value={String(source)} />
              <Row label="Session PnL" value={`${fmt(dashboard?.session_pnl)} ${quoteCcy}`} />
              <Row label="Engine" value={String(dashboard?.engine_status || '—')} />
              <Text style={[styles.help, { marginTop: 6 }]}>
                Capital-survival mode: ~20% free / max 2 USDT per trade. ₹ is approx of USDT, not a second wallet.
              </Text>
              {walletRows.length > 0 && (
                <View style={{ marginTop: 8 }}>
                  <Text style={styles.help}>Delta wallet</Text>
                  {walletRows.map((b) => (
                    <Row
                      key={b.asset}
                      label={b.asset}
                      value={`${fmt(b.available)} avail / ${fmt(b.balance)} total`}
                    />
                  ))}
                </View>
              )}
              <TouchableOpacity style={styles.secondaryBtn} onPress={refreshWallet} activeOpacity={0.8}>
                <Text style={styles.secondaryText} numberOfLines={1}>Refresh from Delta</Text>
              </TouchableOpacity>
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>Position</Text>
              {!pos ? (
                <Text style={styles.help}>
                  {halted ? 'Flat — entries blocked (halted)' : 'Flat — scanning BTC/ETH (strict filters)'}
                </Text>
              ) : (
                <>
                  <Row label="Symbol" value={pos.symbol} />
                  <Row label="Type" value={`${pos.option_type} · K ${fmt(pos.strike, 0)}`} />
                  <Row label="Size" value={String(pos.size)} />
                  <Row label="Delta" value={pos.delta != null ? fmt(pos.delta, 3) : '—'} />
                  <Row label="Entry" value={fmt(pos.entry, 4)} />
                  <Row label="Mark" value={fmt(pos.mark, 4)} />
                  <Row label="uPnL" value={fmt(pos.upnl)} />
                  <Row label="TP / SL" value={`${fmt(pos.tp, 4)} / ${fmt(pos.sl, 4)}`} />
                </>
              )}
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>AI / confidence</Text>
              <Row label="Last action" value={String(dashboard?.ai_last_action || '—')} />
              <Row label="Last conf" value={confPct(dashboard?.ai_confidence)} />
              <Row label="EMERGENCY gate" value="> 95%" />
              <Text style={styles.help}>
                ENTER/EXIT are advisory only. Quant owns entries/exits. AI can force flatten only on EMERGENCY with confidence above 0.95.
              </Text>
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>Exits</Text>
              <Row label="Take profit" value={settings?.tp_display || 'Premium +35%'} />
              <Row label="Stop loss" value={settings?.sl_display || 'Premium −18%'} />
              <Row label="Max hold" value={`${Math.round(Number(settings?.max_hold_sec || dashboard?.max_hold_sec || 14400) / 3600)}h`} />
              <Row label="Underlyings" value={settings?.underlyings || 'BTC,ETH'} />
            </View>

            <TouchableOpacity style={styles.dangerBtn} onPress={kill} activeOpacity={0.8}>
              <Text style={styles.dangerText} numberOfLines={1}>Kill switch</Text>
            </TouchableOpacity>
          </>
        )}

        {activeTab === 'logs' && (
          <View style={[styles.card, { backgroundColor: C.logBg }]}>
            <Text style={styles.cardTitle}>Live engine logs</Text>
            {logs.length === 0 ? (
              <Text style={styles.help}>No logs yet — wait for SCAN / CAPITAL / SIGNAL lines</Text>
            ) : (
              logs.map((l, i) => (
                <Text key={`${l.ts || i}-${i}`} style={styles.logLine} selectable>
                  {l.line}
                </Text>
              ))
            )}
          </View>
        )}

        {activeTab === 'signals' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>AI signals</Text>
            <Text style={styles.help}>
              Confidence gate for EMERGENCY: 95%. Lower-conf ENTER/EXIT do not override the cycle.
            </Text>
            {signals.length === 0 ? (
              <Text style={styles.help}>No AI decisions yet</Text>
            ) : (
              signals.map((s, i) => (
                <View key={`${s.id || s.ts || i}`} style={styles.signalRow}>
                  <View style={styles.signalHead}>
                    <Text style={styles.signalAction}>{s.action || '—'}</Text>
                    <Text style={styles.signalConf}>{confPct(s.confidence)}</Text>
                  </View>
                  <Text style={styles.help}>{s.model || ''} · {s.risk_assessment || ''}</Text>
                  <Text style={styles.signalReason}>{s.reasoning || ''}</Text>
                </View>
              ))
            )}
          </View>
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
            <TouchableOpacity style={styles.primaryBtn} onPress={saveConnection} disabled={saving} activeOpacity={0.8}>
              <Text style={styles.primaryText} numberOfLines={1}>{saving ? 'Saving…' : 'Save & connect'}</Text>
            </TouchableOpacity>
            <Text style={styles.help}>
              Same VPS as futures Rubaih. Greeks uses port 8088; futures uses 8080. Teal icon = Greeks.
            </Text>
          </View>
        )}
      </ScrollView>
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
    paddingHorizontal: 16, paddingTop: 8, paddingBottom: 8,
    flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
    borderBottomWidth: 1, borderBottomColor: C.border,
  },
  brand: { color: C.text, fontSize: 22, fontWeight: '800', letterSpacing: 0.3 },
  sub: { color: C.muted, marginTop: 2, fontSize: 12 },
  pill: { paddingHorizontal: 10, paddingVertical: 6, borderRadius: 999 },
  tabScroll: { flexGrow: 0, borderBottomWidth: 1, borderBottomColor: C.border },
  tabRow: { paddingHorizontal: 12, paddingVertical: 10, alignItems: 'center' },
  tabChip: {
    paddingHorizontal: 14, paddingVertical: 10, borderRadius: 10, minHeight: 40,
    justifyContent: 'center',
    backgroundColor: C.card, borderWidth: 1, borderColor: C.border, marginRight: 8,
  },
  tabChipOn: { backgroundColor: C.accentDim, borderColor: C.accent },
  tabChipText: { color: C.muted, fontWeight: '600', fontSize: 13, includeFontPadding: false },
  tabChipTextOn: { color: C.accent },
  card: {
    backgroundColor: C.card, borderColor: C.border, borderWidth: 1,
    borderRadius: 14, padding: 14, marginBottom: 12,
  },
  cardTitle: { color: C.text, fontSize: 15, fontWeight: '700', marginBottom: 10 },
  help: { color: C.muted, fontSize: 12, lineHeight: 18, marginTop: 8 },
  row: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 8, alignItems: 'flex-start' },
  rowLabel: { color: C.muted, fontSize: 13, flexShrink: 0, paddingRight: 8 },
  rowValue: { color: C.text, fontSize: 13, fontWeight: '600', flex: 1, textAlign: 'right', flexWrap: 'wrap' },
  label: { color: C.muted, fontSize: 12, marginBottom: 6, marginTop: 8 },
  input: {
    backgroundColor: C.inputBg, borderColor: C.border, borderWidth: 1, borderRadius: 10,
    color: C.text, paddingHorizontal: 12, paddingVertical: 12, marginBottom: 4, minHeight: 46,
  },
  primaryBtn: {
    marginTop: 14, backgroundColor: C.accent, borderRadius: 12,
    paddingVertical: 14, paddingHorizontal: 16, minHeight: 48,
    alignItems: 'center', justifyContent: 'center',
  },
  primaryText: { color: '#04221f', fontWeight: '800', fontSize: 15, includeFontPadding: false },
  secondaryBtn: {
    marginTop: 12, borderColor: C.accent, borderWidth: 1, borderRadius: 12,
    paddingVertical: 14, paddingHorizontal: 16, minHeight: 48,
    alignItems: 'center', justifyContent: 'center',
  },
  secondaryText: { color: C.accent, fontWeight: '700', fontSize: 14, includeFontPadding: false },
  dangerBtn: {
    backgroundColor: 'rgba(231,76,60,0.15)', borderColor: C.bad, borderWidth: 1,
    borderRadius: 12, paddingVertical: 14, paddingHorizontal: 16, minHeight: 48,
    alignItems: 'center', justifyContent: 'center', marginTop: 4,
  },
  dangerText: { color: C.bad, fontWeight: '800', fontSize: 15, includeFontPadding: false },
  tradeRow: { paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: C.border },
  tradeMain: { color: C.text, fontWeight: '700', marginBottom: 2 },
  logLine: {
    color: '#b7c4d4', fontSize: 11, fontFamily: 'monospace', lineHeight: 16,
    marginBottom: 6,
  },
  signalRow: { paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: C.border },
  signalHead: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 4 },
  signalAction: { color: C.accent, fontWeight: '800', fontSize: 14 },
  signalConf: { color: C.info, fontWeight: '700' },
  signalReason: { color: C.text, fontSize: 13, marginTop: 4, lineHeight: 18 },
});
