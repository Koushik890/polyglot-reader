import type { Response } from 'express';
import { supabase } from '../config/supabase';
import type { AuthRequest } from '../types/auth';
import { FALLBACK_QUOTES, type QuoteSeed } from '../data/fallbackQuotes';
import { isRealTranslationConfigured, translateText } from '../services/translationService';
import { fetchDailySourceQuote } from '../services/quoteSourceService';

type QuoteRow = {
  id: string | number;
  language_code: string;
  text: string;
  translation: string | null;
  author: string | null;
  source: string | null;
  created_at?: string;
};

type QuoteBaseRow = {
  id: number;
  text_en: string;
  author: string | null;
  source: string | null;
};

type QuoteDailyTranslationRow = {
  date_key: string;
  lang_code: string;
  base_id: number;
  text: string;
  created_at?: string;
};

type QuoteDailyPickRow = {
  date_key: string;
  lang_code?: string;
  base_id: number;
  provider: string | null;
  provider_quote_id: string | null;
  created_at?: string;
};

export type ApiQuote = {
  id?: string | number;
  languageCode: string;
  languageLabel: string;
  flagEmoji: string;
  text: string;
  translation?: string | null;
  author?: string | null;
  source?: string | null;
};

const LANG_META: Record<string, { label: string; flag: string }> = {
  de: { label: 'German', flag: '🇩🇪' },
  en: { label: 'English', flag: '🇺🇸' },
  es: { label: 'Spanish', flag: '🇪🇸' },
  fr: { label: 'French', flag: '🇫🇷' },
  it: { label: 'Italian', flag: '🇮🇹' },
  ru: { label: 'Russian', flag: '🇷🇺' },
  bn: { label: 'Bengali', flag: '🇧🇩' },
};

const normalizeLang = (input: unknown): string | null => {
  if (typeof input !== 'string') return null;
  const trimmed = input.trim().toLowerCase();
  if (!trimmed) return null;
  // Keep only a simple BCP-47-ish primary tag (e.g., "pt-BR" -> "pt")
  const primary = trimmed.split(/[-_]/)[0];
  return primary || null;
};

const parseCsv = (value: unknown): string[] => {
  if (typeof value !== 'string') return [];
  return value
    .split(',')
    .map((s) => normalizeLang(s))
    .filter((s): s is string => Boolean(s));
};

const unique = (items: string[]) => Array.from(new Set(items));

const utcDateKey = (d = new Date()) => d.toISOString().slice(0, 10); // YYYY-MM-DD in UTC

// FNV-1a 32-bit hash (stable, fast, no dependencies)
const hash32 = (input: string) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
};

// In-memory daily cache to avoid repeated DB hits per lang/day
let cacheDay = '';
const dailyCache = new Map<string, ApiQuote>();

const toApiQuote = (seed: QuoteSeed): ApiQuote => ({
  languageCode: seed.languageCode,
  languageLabel: seed.languageLabel,
  flagEmoji: seed.flagEmoji,
  text: seed.text,
  translation: seed.translation ?? null,
  author: seed.author ?? null,
  source: seed.source ?? null,
});

const chooseFallback = (languageCode: string, stableSeed: string): ApiQuote => {
  const forLang = FALLBACK_QUOTES.filter((q) => q.languageCode === languageCode);
  const pool = forLang.length ? forLang : FALLBACK_QUOTES;
  const idx = pool.length ? hash32(stableSeed) % pool.length : 0;
  const chosen = pool[idx] ?? FALLBACK_QUOTES[0];
  return toApiQuote(chosen!);
};

const fetchDailyQuoteFromDb = async (languageCode: string, stableSeed: string): Promise<ApiQuote | null> => {
  // Count rows for this language so we can pick a stable offset.
  const countResp = await supabase
    .from('quotes')
    .select('id', { count: 'exact', head: true })
    .eq('language_code', languageCode);

  if (countResp.error) {
    // If the table doesn't exist or permission is missing, fallback.
    return null;
  }

  const count = countResp.count ?? 0;
  if (count <= 0) return null;

  const offset = hash32(stableSeed) % count;

  const rowResp = await supabase
    .from('quotes')
    .select('id,language_code,text,translation,author,source,created_at')
    .eq('language_code', languageCode)
    .order('id', { ascending: true })
    .range(offset, offset)
    .maybeSingle<QuoteRow>();

  if (rowResp.error || !rowResp.data) return null;

  const meta = LANG_META[languageCode] ?? { label: languageCode.toUpperCase(), flag: '🏳️' };

  return {
    id: rowResp.data.id,
    languageCode: languageCode,
    languageLabel: meta.label,
    flagEmoji: meta.flag,
    text: rowResp.data.text,
    translation: rowResp.data.translation,
    author: rowResp.data.author,
    source: rowResp.data.source,
  };
};

const getOrCreateDailyBaseQuote = async (
  dateKey: string,
  languageCode: string,
  excludeTexts?: Set<string>
): Promise<QuoteBaseRow | null> => {
  // 1) Try existing daily pick (most requests).
  const pickResp = await supabase
    .from('quote_daily_picks')
    .select('date_key,base_id,provider,provider_quote_id,created_at')
    .eq('date_key', dateKey)
    .eq('lang_code', languageCode)
    .maybeSingle<QuoteDailyPickRow>();

  if (!pickResp.error && pickResp.data?.base_id) {
    const baseResp = await supabase
      .from('quote_bases')
      .select('id,text_en,author,source')
      .eq('id', pickResp.data.base_id)
      .maybeSingle<QuoteBaseRow>();
    if (!baseResp.error && baseResp.data) return baseResp.data;
  }

  // 2) No pick yet: fetch from external provider once per day and persist.
  try {
    const src = await fetchDailySourceQuote({ dateKey, languageCode, excludeTexts });

    const baseUpsert = await supabase
      .from('quote_bases')
      .upsert(
        {
          text_en: src.text,
          author: src.author,
          source: src.source,
        },
        { onConflict: 'text_en' }
      )
      .select('id,text_en,author,source')
      .maybeSingle<QuoteBaseRow>();

    if (baseUpsert.error || !baseUpsert.data) {
      return null;
    }

    // Insert daily pick. If another request won the race, keep the existing pick.
    const insertPick = await supabase
      .from('quote_daily_picks')
      .insert({
        date_key: dateKey,
        lang_code: languageCode,
        base_id: baseUpsert.data.id,
        provider: src.provider,
        provider_quote_id: src.providerQuoteId ?? null,
      })
      .select('date_key,base_id')
      .maybeSingle<QuoteDailyPickRow>();

    if (insertPick.error) {
      // Fetch the winner pick
      const winnerPick = await supabase
        .from('quote_daily_picks')
        .select('date_key,base_id')
        .eq('date_key', dateKey)
        .eq('lang_code', languageCode)
        .maybeSingle<QuoteDailyPickRow>();

      if (!winnerPick.error && winnerPick.data?.base_id) {
        const baseResp = await supabase
          .from('quote_bases')
          .select('id,text_en,author,source')
          .eq('id', winnerPick.data.base_id)
          .maybeSingle<QuoteBaseRow>();
        if (!baseResp.error && baseResp.data) return baseResp.data;
      }
    }

    return baseUpsert.data;
  } catch (e) {
    console.error('[quotes] Failed to fetch daily quote from provider', e);
    // Provider might be down or blocked (DNS). Fall back to a curated local bank of REAL quotes
    // stored in `quote_bases` (seeded via migrations).
    const countResp = await supabase
      .from('quote_bases')
      .select('id', { count: 'exact', head: true })
      .neq('source', 'Polyglot Reader')
      .not('author', 'is', null);

    if (countResp.error) return null;
    const count = countResp.count ?? 0;
    if (count <= 0) return null;

    const offset = hash32(`${dateKey}:${languageCode}`) % count;
    const baseResp = await supabase
      .from('quote_bases')
      .select('id,text_en,author,source')
      .neq('source', 'Polyglot Reader')
      .not('author', 'is', null)
      .order('id', { ascending: true })
      .range(offset, offset)
      .maybeSingle<QuoteBaseRow>();

    if (baseResp.error || !baseResp.data) return null;

    // Best-effort daily pick insert for consistency.
    await supabase.from('quote_daily_picks').upsert({
      date_key: dateKey,
      lang_code: languageCode,
      base_id: baseResp.data.id,
      provider: 'curated',
      provider_quote_id: null,
    }, { onConflict: 'date_key,lang_code' });

    return baseResp.data;
  }
};

const fetchOrCreateDailyTranslation = async (base: QuoteBaseRow, lang: string, dateKey: string): Promise<string> => {
  // English requires no translation.
  if (lang === 'en') return base.text_en;

  // Try cached DB translation first.
  const existingResp = await supabase
    .from('quote_daily_translations')
    .select('date_key,lang_code,base_id,text,created_at')
    .eq('date_key', dateKey)
    .eq('lang_code', lang)
    .maybeSingle<QuoteDailyTranslationRow>();

  if (!existingResp.error && existingResp.data && existingResp.data.base_id === base.id) {
    return existingResp.data.text;
  }

  // Compute translation via configured provider.
  const out = await translateText(base.text_en, 'en', lang);
  const translated = typeof out?.translation === 'string' && out.translation ? out.translation : base.text_en;

  // Best-effort cache write (requires the table + correct permissions).
  await supabase
    .from('quote_daily_translations')
    .upsert(
      {
        date_key: dateKey,
        lang_code: lang,
        base_id: base.id,
        text: translated,
      },
      { onConflict: 'date_key,lang_code' }
    );

  return translated;
};

const getDailyQuote = async (
  languageCode: string,
  dateKey: string,
  userLangHint?: string | null,
  excludeTexts?: Set<string>
): Promise<ApiQuote> => {
  // Reset cache when day changes
  if (cacheDay !== dateKey) {
    cacheDay = dateKey;
    dailyCache.clear();
  }

  const cacheKey = `${languageCode}:${dateKey}`;
  const cached = dailyCache.get(cacheKey);
  if (cached) return cached;

  // Preferred path: one global English base quote per day, translated to each language.
  const base = await getOrCreateDailyBaseQuote(dateKey, languageCode, excludeTexts);
  if (base && isRealTranslationConfigured()) {
    const meta = LANG_META[languageCode] ?? { label: languageCode.toUpperCase(), flag: '🏳️' };
    const text = await fetchOrCreateDailyTranslation(base, languageCode, dateKey);
    const chosen: ApiQuote = {
      id: `${base.id}:${languageCode}:${dateKey}`,
      languageCode,
      languageLabel: meta.label,
      flagEmoji: meta.flag,
      text,
      translation: languageCode === 'en' ? null : base.text_en,
      author: base.author,
      source: base.source,
    };
    dailyCache.set(cacheKey, chosen);
    return chosen;
  }

  // Fallback path: per-language quotes table (no translation provider needed).
  const stableSeed = `${languageCode}:${dateKey}`;
  const fromDb = await fetchDailyQuoteFromDb(languageCode, stableSeed);
  const chosen = fromDb ?? chooseFallback(languageCode, stableSeed);

  dailyCache.set(cacheKey, chosen);
  return chosen;
};

export const getQuoteDaily = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const user = req.user;
    if (!user?.id) {
      res.status(401).json({ message: 'Not authorized' });
      return;
    }

    const dateKey = utcDateKey();
    const queryLang = normalizeLang(req.query.lang);
    const userLang = normalizeLang((user.settings ?? {})['translationLanguage']);
    const languageCode = queryLang ?? userLang ?? 'en';

    const quote = await getDailyQuote(languageCode, dateKey, userLang);

    res.setHeader('Cache-Control', 'private, max-age=3600');
    res.json({ date: dateKey, quote });
  } catch (error) {
    console.error('getQuoteDaily failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

export const getQuotesFeatured = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const user = req.user;
    if (!user?.id) {
      res.status(401).json({ message: 'Not authorized' });
      return;
    }

    const dateKey = utcDateKey();

    const requested = parseCsv(req.query.langs ?? req.query.languages);
    const userLang = normalizeLang((user.settings ?? {})['translationLanguage']);
    const defaultLangs = unique(
      [userLang, 'de', 'en', 'es', 'fr', 'it', 'ru', 'bn'].filter((v): v is string => Boolean(v))
    );

    const langs = unique(requested.length ? requested : defaultLangs).slice(0, 7);

    // Ensure different languages get different quotes whenever possible.
    const usedBaseTexts = new Set<string>();
    const quotes: ApiQuote[] = [];
    for (const lang of langs) {
      const q = await getDailyQuote(lang, dateKey, userLang, usedBaseTexts);
      // Track the English base (translation field contains English when non-English)
      if (q.translation && typeof q.translation === 'string') usedBaseTexts.add(q.translation);
      else usedBaseTexts.add(q.text);
      quotes.push(q);
    }

    res.setHeader('Cache-Control', 'private, max-age=3600');
    res.json({ date: dateKey, quotes });
  } catch (error) {
    console.error('getQuotesFeatured failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

