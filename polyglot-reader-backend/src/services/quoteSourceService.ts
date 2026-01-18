type Provider = 'typefit' | 'favqs_qotd' | 'quotable';

export type SourceQuote = {
  text: string;
  author: string | null;
  source: string | null;
  provider: Provider;
  providerQuoteId?: string | null;
};

const requireFetch = () => {
  const f = (globalThis as any).fetch as undefined | typeof fetch;
  if (!f) {
    throw new Error('Global fetch is not available. Use Node 18+ or add a fetch polyfill.');
  }
  return f;
};

const normalizeBaseUrl = (value: string) => value.replace(/\/+$/, '');

const QUOTABLE_BASE = normalizeBaseUrl(process.env.QUOTABLE_API_BASE || 'https://api.quotable.io');
const QUOTABLE_TAGS =
  (process.env.QUOTABLE_TAGS || 'education|learning|books|reading|literature|wisdom').trim();
const QUOTABLE_MAX_LENGTH = Math.max(60, Math.min(220, parseInt(process.env.QUOTABLE_MAX_LENGTH || '140', 10) || 140));

const FAVQS_QOTD_URL = (process.env.FAVQS_QOTD_URL || 'https://favqs.com/api/qotd').trim();

const TYPEFIT_QUOTES_URL = (process.env.TYPEFIT_QUOTES_URL || 'https://type.fit/api/quotes').trim();
const TYPEFIT_MAX_LENGTH = Math.max(60, Math.min(240, parseInt(process.env.TYPEFIT_MAX_LENGTH || '160', 10) || 160));

const fetchJson = async (url: string) => {
  const fetch = requireFetch();
  const resp = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'User-Agent': 'polyglot-reader-backend/1.0',
    },
  });
  const text = await resp.text().catch(() => '');
  if (!resp.ok) {
    throw new Error(`Quote provider failed (${resp.status}): ${text.slice(0, 400)}`);
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Quote provider returned non-JSON: ${text.slice(0, 200)}`);
  }
};

// FNV-1a 32-bit hash (stable, fast, no dependencies)
const hash32 = (input: string) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
};

const looksLikeFullName = (author: string | null) => {
  if (!author) return false;
  const a = author.trim();
  if (!a) return false;
  // Must be at least two words.
  if (!/\s/.test(a)) return false;
  const lower = a.toLowerCase();
  // Avoid non-person placeholders.
  const banned = ['anonymous', 'unknown', 'proverb', 'anon', 'various'];
  if (banned.some((b) => lower.includes(b))) return false;
  return true;
};

const isLearningRelated = (text: string) => {
  const t = text.toLowerCase();
  const keywords = [
    'read',
    'reading',
    'book',
    'books',
    'learn',
    'learning',
    'education',
    'study',
    'studying',
    'teacher',
    'teachers',
    'student',
    'students',
    'language',
    'languages',
    'vocabulary',
    'word',
    'words',
    'knowledge',
    'wisdom',
    'write',
    'writing',
    'literature',
    'library',
  ];
  return keywords.some((k) => t.includes(k));
};

type TypeFitQuote = { text?: unknown; author?: unknown };
let typefitCache: { fetchedAt: number; quotes: Array<{ text: string; author: string | null }> } | null = null;

const loadTypeFitQuotes = async () => {
  const now = Date.now();
  // Cache list for 12 hours.
  if (typefitCache && now - typefitCache.fetchedAt < 1000 * 60 * 60 * 12) return typefitCache.quotes;

  const data: any = await fetchJson(TYPEFIT_QUOTES_URL);
  if (!Array.isArray(data)) {
    throw new Error('Type.fit returned unexpected response');
  }

  const quotes = (data as TypeFitQuote[])
    .map((q) => {
      const text = typeof q?.text === 'string' ? q.text.trim() : '';
      const authorRaw = typeof q?.author === 'string' ? q.author.trim() : '';
      return { text, author: authorRaw || null };
    })
    .filter((q) => q.text);

  typefitCache = { fetchedAt: now, quotes };
  return quotes;
};

const fetchFromTypeFit = async (seed: string, excludeTexts?: Set<string>): Promise<SourceQuote> => {
  const all = await loadTypeFitQuotes();

  // Strong filtering first: related + full name + reasonable length
  const filtered = all
    .filter((q) => q.text.length <= TYPEFIT_MAX_LENGTH)
    .filter((q) => looksLikeFullName(q.author))
    .filter((q) => isLearningRelated(q.text))
    .filter((q) => (excludeTexts ? !excludeTexts.has(q.text) : true));

  // If too strict, relax "learning related" but keep full-name requirement.
  const relaxed = all
    .filter((q) => q.text.length <= TYPEFIT_MAX_LENGTH)
    .filter((q) => looksLikeFullName(q.author))
    .filter((q) => (excludeTexts ? !excludeTexts.has(q.text) : true));

  const pool = filtered.length ? filtered : relaxed;
  if (!pool.length) throw new Error('Type.fit pool is empty after filtering');

  // Deterministic pick for the day + language, but still "random enough" and stable.
  const start = hash32(seed) % pool.length;
  const chosen = pool[start]!;

  return {
    text: chosen.text,
    author: chosen.author,
    source: 'Type.fit',
    provider: 'typefit',
    providerQuoteId: null,
  };
};

const fetchFromFavQs = async (): Promise<SourceQuote> => {
  const data: any = await fetchJson(FAVQS_QOTD_URL);
  const body = typeof data?.quote?.body === 'string' ? data.quote.body.trim() : '';
  const author = typeof data?.quote?.author === 'string' ? data.quote.author.trim() : null;
  const id = data?.quote?.id != null ? String(data.quote.id) : null;

  if (!body) {
    throw new Error('FavQs returned an empty quote');
  }

  // If author isn't a full name, treat it as a failure so we can fall back to another provider.
  if (!looksLikeFullName(author)) {
    throw new Error(`FavQs author is not a full name: ${author || '(missing)'}`);
  }

  // If quote isn't related to learning/reading, treat as failure.
  if (!isLearningRelated(body)) {
    throw new Error('FavQs quote not related to learning/reading');
  }

  return {
    text: body,
    author: author || null,
    source: 'FavQs',
    provider: 'favqs_qotd',
    providerQuoteId: id,
  };
};

const fetchFromQuotable = async (): Promise<SourceQuote> => {
  // Try topic-related tags first; if no match, fall back to an untagged random quote.
  // (Quotable supports OR tags using `|` in many deployments; if this yields 404, we retry without tags.)
  // Try topic-related tags first; if no match, fall back to an untagged random quote.
  // (Quotable supports OR tags using `|` in many deployments; if this yields 404, we retry without tags.)
  const baseUrl = QUOTABLE_BASE;
  const withTags = `${baseUrl}/random?tags=${encodeURIComponent(QUOTABLE_TAGS)}&maxLength=${encodeURIComponent(
    String(QUOTABLE_MAX_LENGTH)
  )}`;

  let data: any;
  try {
    data = await fetchJson(withTags);
  } catch (e) {
    console.warn('[quotes] Tagged fetch failed, retrying without tags', e);
    const withoutTags = `${baseUrl}/random?maxLength=${encodeURIComponent(String(QUOTABLE_MAX_LENGTH))}`;
    data = await fetchJson(withoutTags);
  }

  const text = typeof data?.content === 'string' ? data.content.trim() : '';
  const author = typeof data?.author === 'string' ? data.author.trim() : null;
  const id = typeof data?._id === 'string' ? data._id : null;

  if (!text) {
    throw new Error('Quote provider returned an empty quote');
  }

  if (!looksLikeFullName(author)) {
    throw new Error(`Quotable author is not a full name: ${author || '(missing)'}`);
  }

  return {
    text,
    author: author || null,
    source: 'Quotable',
    provider: 'quotable',
    providerQuoteId: id,
  };
};

export const fetchDailySourceQuote = async ({
  dateKey,
  languageCode,
  excludeTexts,
}: {
  dateKey: string;
  languageCode: string;
  excludeTexts?: Set<string>;
}): Promise<SourceQuote> => {
  const seed = `${dateKey}:${languageCode}`;

  // Prefer a reachable provider; allow override via env.
  const provider = (process.env.QUOTE_SOURCE_PROVIDER || '').toLowerCase().trim();
  if (provider === 'typefit') return fetchFromTypeFit(seed, excludeTexts);
  if (provider === 'quotable') return fetchFromQuotable();
  if (provider === 'favqs' || provider === 'favqs_qotd') return fetchFromFavQs();

  // Default priority: Type.fit (reachable + filterable) -> FavQs -> Quotable
  try {
    return await fetchFromTypeFit(seed, excludeTexts);
  } catch (e) {
    console.warn('[quotes] Type.fit failed, falling back to FavQs', e);
    try {
      return await fetchFromFavQs();
    } catch (e2) {
      console.warn('[quotes] FavQs failed, falling back to Quotable', e2);
      return await fetchFromQuotable();
    }
  }
};

