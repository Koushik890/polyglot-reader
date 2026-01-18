type ChatMessage = { role: 'system' | 'user' | 'assistant'; content: string };

const requireFetch = () => {
  const f = (globalThis as any).fetch as undefined | typeof fetch;
  if (!f) {
    throw new Error('Global fetch is not available. Use Node 18+ or add a fetch polyfill.');
  }
  return f;
};

const clampNum = (raw: unknown, min: number, max: number, fallback: number) => {
  const n = typeof raw === 'string' ? parseFloat(raw) : typeof raw === 'number' ? raw : NaN;
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
};

const clampInt = (raw: unknown, min: number, max: number, fallback: number) => {
  const n = typeof raw === 'string' ? parseInt(raw, 10) : typeof raw === 'number' ? raw : NaN;
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
};

const withTimeout = async (input: string, init: RequestInit, timeoutMs: number) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const fetch = requireFetch();
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
};

export type MistralChatOptions = {
  model?: string;
  temperature?: number;
  top_p?: number;
  max_tokens?: number;
  timeoutMs?: number;
};

export const mistralChat = async (messages: ChatMessage[], opts?: MistralChatOptions): Promise<string> => {
  const apiKey = (process.env.MISTRAL_API_KEY || '').trim();
  if (!apiKey) throw new Error('Missing MISTRAL_API_KEY');

  const model = (opts?.model || process.env.MISTRAL_MODEL || 'mistral-large-latest').trim();
  const temperature = clampNum(opts?.temperature ?? process.env.MISTRAL_TEMPERATURE, 0, 1.5, 0.4);
  const top_p = clampNum(opts?.top_p ?? process.env.MISTRAL_TOP_P, 0.1, 1, 1);
  const max_tokens = clampInt(opts?.max_tokens ?? process.env.MISTRAL_MAX_TOKENS, 64, 2048, 240);
  const timeoutMs = clampInt(opts?.timeoutMs ?? process.env.MISTRAL_TIMEOUT_MS, 1500, 20000, 9000);

  const body = {
    model,
    messages,
    temperature,
    top_p,
    max_tokens,
  };

  const resp = await withTimeout(
    'https://api.mistral.ai/v1/chat/completions',
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(body),
    },
    timeoutMs
  );

  if (!resp.ok) {
    const raw = await resp.text().catch(() => '');
    throw new Error(`Mistral API failed (${resp.status}): ${raw.slice(0, 500)}`);
  }

  const json = (await resp.json()) as any;
  const content = json?.choices?.[0]?.message?.content;
  if (typeof content !== 'string' || !content.trim()) {
    throw new Error('Mistral API returned empty response');
  }
  return content.trim();
};

