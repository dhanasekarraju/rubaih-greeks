# Mobile config for Rubaih Greeks
# Same VPS as Rubaih futures — Greeks uses nginx :8088 (futures uses :8080).

export const API_BASE = "http://103.194.228.130:8088";

// Must match RUBAIH_GREEKS_API_TOKEN in VPS ~/rubaih-greeks/.env
export const API_TOKEN = "REPLACE_WITH_RUBAIH_GREEKS_API_TOKEN";
