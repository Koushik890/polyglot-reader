type ConversationInputEntry = {
  role: 'user';
  content: string;
  object: 'entry';
  type: 'message.input';
};

const requireFetch = () => {
  const f = (globalThis as any).fetch as undefined | typeof fetch;
  if (!f) {
    throw new Error('Global fetch is not available. Use Node 18+ or add a fetch polyfill.');
  }
  return f;
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

export type MistralAgentRunOptions = {
  agentId: string;
  prompt: string;
  /**
   * If false, the conversation won't be stored on Mistral's side (recommended for per-request summaries).
   * Docs mention `store=False` to opt out.
   */
  store?: boolean;
  timeoutMs?: number;
};

export const mistralAgentRunOnce = async (opts: MistralAgentRunOptions): Promise<string> => {
  const apiKey = (process.env.MISTRAL_API_KEY || '').trim();
  if (!apiKey) throw new Error('Missing MISTRAL_API_KEY');

  const agentId = (opts.agentId || '').trim();
  if (!agentId) throw new Error('Missing agentId');

  const timeoutMs = clampInt(opts.timeoutMs ?? process.env.MISTRAL_TIMEOUT_MS, 1500, 20000, 9000);
  const store = typeof opts.store === 'boolean' ? opts.store : false;

  const inputs: ConversationInputEntry[] = [
    {
      role: 'user',
      content: String(opts.prompt || '').trim(),
      object: 'entry',
      type: 'message.input',
    },
  ];

  const body = {
    inputs,
    stream: false,
    agent_id: agentId,
    // Docs: opt out from automatic storing with store=False.
    store,
  };

  const resp = await withTimeout(
    'https://api.mistral.ai/v1/conversations',
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
    throw new Error(`Mistral Agents API failed (${resp.status}): ${raw.slice(0, 500)}`);
  }

  const json = (await resp.json()) as any;
  const outputs = Array.isArray(json?.outputs) ? json.outputs : [];
  const assistant = outputs
    .filter((o: any) => o && typeof o === 'object')
    .reverse()
    .find((o: any) => o?.type === 'message.output' && o?.role === 'assistant' && typeof o?.content === 'string');

  const content = typeof assistant?.content === 'string' ? assistant.content.trim() : '';
  if (!content) throw new Error('Mistral Agents API returned empty output');
  return content;
};

