/**
 * Example only — copy to config.js for local APK builds.
 * config.js is gitignored (contains token).
 */

export const DEFAULT_API_HOST = 'http://103.194.228.130:8088';
export const DEFAULT_API_TOKEN = 'REPLACE_WITH_RUBAIH_GREEKS_API_TOKEN';

export function normalizeHost(host) {
  let h = (host || '').trim().replace(/\/+$/, '');
  if (!h) return '';
  if (!/^https?:\/\//i.test(h)) h = `http://${h}`;
  h = h.replace(/:(8000|8010|8018)$/, '');
  return h;
}

export function buildUrls(host, token) {
  const apiHost = normalizeHost(host);
  const apiToken = (token || '').trim();
  return {
    apiHost,
    apiToken,
    apiUrl: `${apiHost}/api`,
    wsUrl: `${apiHost.replace(/^http/i, 'ws')}/ws?token=${encodeURIComponent(apiToken)}`,
    authHeaders: {
      'Content-Type': 'application/json',
      'X-API-Token': apiToken,
    },
    configured: Boolean(
      apiHost &&
      !apiHost.includes('YOUR_VPS_IP') &&
      apiToken &&
      !apiToken.includes('REPLACE_WITH')
    ),
  };
}
