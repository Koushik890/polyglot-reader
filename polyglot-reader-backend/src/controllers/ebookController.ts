import type { Response } from 'express';
import type { AuthRequest } from '../types/auth';
import { supabase } from '../config/supabase';

type EbookSource = 'gutendex' | 'standardebooks' | 'wikisource' | 'wolnelektury' | 'manybooks';

export type ApiEbook = {
  id: string;
  title: string;
  author: string;
  language: string;
  category: string;
  coverUrl?: string | null;
  downloadUrl?: string | null;
  // Not returned to clients (we do not expose source per requirements)
  _source?: EbookSource;
  _popularity?: number;
};

type ApiTopAuthor = { name: string; count: number };

type ApiEbooksResponse = {
  lang: string;
  generatedAt: string;
  categories: string[];
  topAuthors: ApiTopAuthor[];
  trending: ApiEbook[];
  byCategory: Record<string, ApiEbook[]>;
};

const normalizeLang = (input: unknown): string | null => {
  if (typeof input !== 'string') return null;
  const trimmed = input.trim().toLowerCase();
  if (!trimmed) return null;
  const primary = trimmed.split(/[-_]/)[0];
  return primary || null;
};

const clampInt = (raw: unknown, min: number, max: number, fallback: number) => {
  const n = typeof raw === 'string' ? parseInt(raw, 10) : typeof raw === 'number' ? raw : NaN;
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
};

const toUtcDateParts = (d: Date) => {
  const yyyy = String(d.getUTCFullYear());
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return { yyyy, mm, dd };
};

const utcDaysAgo = (daysAgo: number) => new Date(Date.now() - Math.max(0, daysAgo) * 24 * 60 * 60 * 1000);

const decodeXmlEntities = (value: string) =>
  value
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");

const uniq = <T,>(items: T[]) => Array.from(new Set(items));

const CATEGORY_ORDER: string[] = [
  'Travel',
  'Adventure',
  'Mystery',
  'Horror',
  'Fantasy',
  'Science Fiction',
  'Mythic',
  "Children's",
  'Romance',
  'Short Stories',
  'Drama',
  'Comedy',
  'Biography',
  'History',
  'Western',
  'Poetry',
  'Fiction',
  'Nonfiction',
];

const normalizeApostrophes = (value: string) => value.replace(/\u2019/g, "'"); // ’ -> '
const decodeDbKey = (value: string) => {
  try {
    return decodeURIComponent((value || '').replace(/_/g, ' '));
  } catch {
    return (value || '').replace(/_/g, ' ');
  }
};

const buildWikisourceExportEpubUrl = (lang: string, title: string) => {
  // WSExport generates downloadable EPUBs from Wikisource pages.
  // NOTE: This is an external best-effort service and may be temporarily unavailable.
  const page = String(title || '').trim().replace(/\s+/g, '_');
  const url = new URL('https://ws-export.wmcloud.org/');
  url.searchParams.set('lang', lang);
  url.searchParams.set('page', page);
  url.searchParams.set('format', 'epub');
  return url.toString();
};

const authorFromWikisourceCategories = (categories: string[]) => {
  // High-confidence patterns only (explicit author namespace categories).
  // Keep conservative to avoid wrong attributions.
  for (const raw of categories || []) {
    const c = String(raw || '').trim();
    if (!c) continue;
    const m = c.match(/^(Author|Autor|Auteur|Автор|লেখক)\s*:\s*(.+)$/i);
    if (!m) continue;
    const name = String(m[2] || '').trim();
    if (!name || name.length < 3) continue;
    if (name.toLowerCase() === 'unknown') continue;
    return name;
  }
  return null;
};

const escapeRegex = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const WS_SITEINFO_TTL_MS = 14 * 24 * 60 * 60 * 1000;
const wsAuthorNsCache = new Map<string, { expiresAt: number; names: string[] }>();

const getWikisourceAuthorNamespaceNames = async (lang: string): Promise<string[]> => {
  const key = String(lang || '').toLowerCase() || 'en';
  const now = Date.now();
  const cached = wsAuthorNsCache.get(key);
  if (cached && cached.expiresAt > now) return cached.names;

  try {
    const url = new URL(`https://${key}.wikisource.org/w/api.php`);
    url.searchParams.set('action', 'query');
    url.searchParams.set('format', 'json');
    url.searchParams.set('meta', 'siteinfo');
    url.searchParams.set('siprop', 'namespaces|namespacealiases');

    const resp = await withTimeout(
      url.toString(),
      {
        headers: { accept: 'application/json', 'user-agent': 'PolyglotReader/1.0 (+https://polyglot.local)' },
      },
      3500
    );
    if (!resp.ok) throw new Error(`siteinfo failed: ${resp.status}`);
    const json = (await resp.json()) as any;
    const ns = json?.query?.namespaces ?? {};
    const ns102 = ns?.['102'] || ns?.[102] || null;
    const primary = typeof ns102?.['*'] === 'string' ? ns102['*'] : null;
    const canonical = typeof ns102?.canonical === 'string' ? ns102.canonical : null;
    const aliases = Array.isArray(json?.query?.namespacealiases)
      ? json.query.namespacealiases
          .filter((a: any) => String(a?.id) === '102')
          .map((a: any) => a?.['*'])
          .filter((v: any) => typeof v === 'string' && v.trim().length > 0)
      : [];

    const names = Array.from(
      new Set(
        [primary, canonical, 'Author', ...aliases]
          .filter((v): v is string => typeof v === 'string' && v.trim().length > 0)
          .map((v) => v.trim())
      )
    );
    wsAuthorNsCache.set(key, { expiresAt: now + WS_SITEINFO_TTL_MS, names });
    return names;
  } catch {
    const fallback = ['Author'];
    wsAuthorNsCache.set(key, { expiresAt: now + 6 * 60 * 60 * 1000, names: fallback });
    return fallback;
  }
};

const parseAuthorFromWikitext = (wikitext: string, authorNsNames: string[], lang: string): string | null => {
  const snippet = String(wikitext || '').slice(0, 12000);
  if (!snippet) return null;

  const aliases = (authorNsNames || []).filter(Boolean);
  const nsAlt = aliases.length ? aliases.map(escapeRegex).sort((a, b) => b.length - a.length).join('|') : 'Author';
  const authorLinkRe = new RegExp(`\\[\\[\\s*(?:${nsAlt})\\s*:\\s*([^\\]|#]+)`, 'i');

  // 1) Try explicit template params near the top (multi-language, common keys)
  const paramRe = /\|\s*(author|auteur|autor|автор|লেখক)\s*=\s*([^\|\}\n]+)/gi;
  for (const m of snippet.matchAll(paramRe)) {
    const raw = String(m?.[2] || '').trim();
    if (!raw) continue;
    const lm = raw.match(authorLinkRe);
    const candidate = (lm?.[1] || raw)
      .replace(/\[\[|\]\]/g, '')
      .replace(/''+/g, '')
      .replace(/<ref[\s\S]*?<\/ref>/gi, '')
      .replace(/<[^>]+>/g, '')
      .replace(/\{\{[\s\S]*?\}\}/g, '')
      .replace(/_/g, ' ')
      .trim();
    const cleaned = normalizeAuthorName(candidate);
    if (cleaned && cleaned.toLowerCase() !== 'unknown') return cleaned;
  }

  // 2) Otherwise, accept the first author-namespace link if present near top.
  const lm = snippet.match(authorLinkRe);
  if (lm?.[1]) {
    const candidate = String(lm[1]).replace(/_/g, ' ').trim();
    const cleaned = normalizeAuthorName(candidate);
    if (cleaned && cleaned.toLowerCase() !== 'unknown') return cleaned;
  }

  return null;
};

const fetchWikisourceWikitextAuthors = async (lang: string, titles: string[]): Promise<Record<string, string>> => {
  const out: Record<string, string> = {};
  const list = Array.from(new Set((titles || []).map((t) => String(t || '').trim()).filter(Boolean)));
  if (!list.length) return out;

  const authorNs = await getWikisourceAuthorNamespaceNames(lang);

  const batchSize = 20;
  for (let i = 0; i < list.length; i += batchSize) {
    const batch = list.slice(i, i + batchSize);
    const url = new URL(`https://${lang}.wikisource.org/w/api.php`);
    url.searchParams.set('action', 'query');
    url.searchParams.set('format', 'json');
    url.searchParams.set('formatversion', '2');
    url.searchParams.set('redirects', '1');
    url.searchParams.set('prop', 'revisions');
    url.searchParams.set('rvprop', 'content');
    url.searchParams.set('rvslots', 'main');
    url.searchParams.set('rvlimit', '1');
    url.searchParams.set('titles', batch.join('|'));

    const resp = await withTimeout(
      url.toString(),
      {
        headers: { accept: 'application/json', 'user-agent': 'PolyglotReader/1.0 (+https://polyglot.local)' },
      },
      4500
    );
    if (!resp.ok) continue;
    const json = (await resp.json()) as any;
    const pages = Array.isArray(json?.query?.pages) ? json.query.pages : [];
    for (const p of pages) {
      const title = typeof p?.title === 'string' ? p.title.trim() : '';
      if (!title) continue;
      const rev = Array.isArray(p?.revisions) ? p.revisions[0] : null;
      const content = rev?.slots?.main?.content;
      const wikitext = typeof content === 'string' ? content : '';
      if (!wikitext) continue;
      const author = parseAuthorFromWikitext(wikitext, authorNs, lang);
      if (author) out[title] = author;
    }
  }

  return out;
};

// --- Cover enrichment (keeps book content from original libraries, upgrades cover art when possible) ---
// NOTE: Cover images can be copyrighted even when the text is public domain. This feature prioritizes
// better UX but should be reviewed for your product/legal requirements.

const normalizeForLookup = (value: string) => {
  const raw = (value || '').trim().toLowerCase();
  if (!raw) return '';
  // Remove diacritics (NFKD -> strip combining marks)
  const noMarks = raw.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  // Keep alphanumerics and spaces only, collapse whitespace.
  return noMarks
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
};

const isProbablyGenericGutenbergCover = (url: string | null | undefined) => {
  const u = String(url || '').toLowerCase();
  if (!u) return false;
  // Many Gutenberg "covers" are auto-generated title pages and live under /cache/epub/...cover...
  return u.includes('gutenberg.org') && u.includes('/cache/epub/') && u.includes('cover');
};

const withTimeout = async (input: string, init: RequestInit, timeoutMs: number) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(input, { ...init, signal: controller.signal });
    return resp;
  } finally {
    clearTimeout(timer);
  }
};

type OpenLibrarySearchDoc = {
  title?: string;
  author_name?: string[];
  cover_i?: number;
};

type OpenLibrarySearchResponse = {
  docs?: OpenLibrarySearchDoc[];
};

const OPEN_LIBRARY_UA = 'PolyglotReader/1.0 (metadata-enrichment)';
const OPEN_LIBRARY_TIMEOUT_MS = 3500;
const COVER_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const olCache = new Map<string, { expiresAt: number; coverUrl: string | null; author: string | null; title: string | null }>();
const olInFlight = new Map<string, Promise<{ coverUrl: string | null; author: string | null; title: string | null }>>();

// --- Wikidata author enrichment (for Wikisource items missing author metadata) ---
const WIKIDATA_UA = 'PolyglotReader/1.0 (wikidata-author-enrichment)';
const WIKIDATA_TIMEOUT_MS = 3500;
const WORK_AUTHOR_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const workAuthorCache = new Map<string, { expiresAt: number; author: string | null }>();

const isValidQid = (value: unknown): value is string => typeof value === 'string' && /^Q\d+$/.test(value.trim());

const pickLabel = (labels: any, preferredLangs: string[]) => {
  const order = preferredLangs.filter(Boolean);
  for (const lang of order) {
    const v = labels?.[lang]?.value;
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  // fallback: any label
  const first = labels && typeof labels === 'object' ? Object.values(labels)[0] : null;
  const fv = (first as any)?.value;
  return typeof fv === 'string' && fv.trim() ? fv.trim() : null;
};

const extractFirstClaimEntityId = (entity: any, prop: string): string | null => {
  const claims = entity?.claims?.[prop];
  if (!Array.isArray(claims)) return null;
  for (const c of claims) {
    const id = c?.mainsnak?.datavalue?.value?.id;
    if (isValidQid(id)) return id.trim();
  }
  return null;
};

const extractFirstClaimString = (entity: any, prop: string): string | null => {
  const claims = entity?.claims?.[prop];
  if (!Array.isArray(claims)) return null;
  for (const c of claims) {
    const v = c?.mainsnak?.datavalue?.value;
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return null;
};

const extractAuthorSignalFromWork = (entity: any): { authorEntity: string | null; authorString: string | null; editionOf: string | null } => {
  const authorString = extractFirstClaimString(entity, 'P2093');
  const authorEntity = extractFirstClaimEntityId(entity, 'P50') || extractFirstClaimEntityId(entity, 'P170');
  const editionOf = extractFirstClaimEntityId(entity, 'P629'); // edition or translation of (often points to the underlying work)
  return { authorEntity, authorString, editionOf };
};

const fetchWikidataEntities = async (ids: string[], props: string, languages?: string) => {
  const url = new URL('https://www.wikidata.org/w/api.php');
  url.searchParams.set('action', 'wbgetentities');
  url.searchParams.set('format', 'json');
  url.searchParams.set('ids', ids.join('|'));
  url.searchParams.set('props', props);
  if (languages) url.searchParams.set('languages', languages);
  url.searchParams.set('languagefallback', '1');

  const resp = await withTimeout(
    url.toString(),
    {
      headers: {
        accept: 'application/json',
        'user-agent': WIKIDATA_UA,
      },
    },
    WIKIDATA_TIMEOUT_MS
  );
  if (!resp.ok) return null;
  return (await resp.json()) as any;
};

const getWorkAuthorsFromWikidata = async (workQids: string[], lang: string): Promise<Record<string, string>> => {
  const now = Date.now();
  const unique = Array.from(new Set(workQids.map((q) => q.trim()).filter(isValidQid)));
  if (!unique.length) return {};

  const out: Record<string, string> = {};
  const toFetch: string[] = [];

  for (const qid of unique) {
    const cached = workAuthorCache.get(qid);
    if (cached && cached.expiresAt > now) {
      if (cached.author) out[qid] = cached.author;
    } else {
      toFetch.push(qid);
    }
  }

  // Fetch claims for uncached works, in batches.
  const batchSize = 45;
  for (let i = 0; i < toFetch.length; i += batchSize) {
    const batch = toFetch.slice(i, i + batchSize);
    const json = await fetchWikidataEntities(batch, 'claims');
    const entities = json?.entities && typeof json.entities === 'object' ? json.entities : {};

    const workToAuthorQid = new Map<string, string>();
    const authorQids = new Set<string>();
    const workToEditionOf = new Map<string, string>();
    const editionQids = new Set<string>();

    for (const qid of batch) {
      const ent = entities?.[qid];
      if (!ent) continue;

      const { authorEntity, authorString, editionOf } = extractAuthorSignalFromWork(ent);

      if (authorString) {
        out[qid] = authorString;
        workAuthorCache.set(qid, { expiresAt: now + WORK_AUTHOR_CACHE_TTL_MS, author: authorString });
        continue;
      }

      if (authorEntity) {
        workToAuthorQid.set(qid, authorEntity);
        authorQids.add(authorEntity);
        continue;
      }

      if (editionOf) {
        workToEditionOf.set(qid, editionOf);
        editionQids.add(editionOf);
      }
    }

    // Resolve author entity labels (batch)
    const authorIdList = Array.from(authorQids);
    const authorLabels: Record<string, string> = {};
    for (let j = 0; j < authorIdList.length; j += batchSize) {
      const aBatch = authorIdList.slice(j, j + batchSize);
      const json2 = await fetchWikidataEntities(aBatch, 'labels', `${lang}|en`);
      const ents2 = json2?.entities && typeof json2.entities === 'object' ? json2.entities : {};
      for (const aid of aBatch) {
        const label = pickLabel(ents2?.[aid]?.labels, [lang, 'en']);
        if (label) authorLabels[aid] = label;
      }
    }

    for (const [workId, authorId] of workToAuthorQid.entries()) {
      const name = authorLabels[authorId];
      if (name) {
        out[workId] = name;
        workAuthorCache.set(workId, { expiresAt: now + WORK_AUTHOR_CACHE_TTL_MS, author: name });
      } else {
        workAuthorCache.set(workId, { expiresAt: now + 6 * 60 * 60 * 1000, author: null });
      }
    }

    // Fallback: if the Wikisource item QID represents an *edition*, follow P629 -> work and try again.
    const editionIdList = Array.from(editionQids);
    if (editionIdList.length) {
      const editionToAuthorQid = new Map<string, string>();
      const editionToAuthorString = new Map<string, string>();
      const editionAuthorQids = new Set<string>();

      for (let j = 0; j < editionIdList.length; j += batchSize) {
        const eBatch = editionIdList.slice(j, j + batchSize);
        const jsonE = await fetchWikidataEntities(eBatch, 'claims');
        const entsE = jsonE?.entities && typeof jsonE.entities === 'object' ? jsonE.entities : {};
        for (const eqid of eBatch) {
          const ent = entsE?.[eqid];
          if (!ent) continue;
          const { authorEntity, authorString } = extractAuthorSignalFromWork(ent);
          if (authorString) {
            editionToAuthorString.set(eqid, authorString);
            continue;
          }
          if (authorEntity) {
            editionToAuthorQid.set(eqid, authorEntity);
            editionAuthorQids.add(authorEntity);
          }
        }
      }

      // Resolve labels for edition author entities
      const edAuthorList = Array.from(editionAuthorQids);
      const edAuthorLabels: Record<string, string> = {};
      for (let j = 0; j < edAuthorList.length; j += batchSize) {
        const aBatch = edAuthorList.slice(j, j + batchSize);
        const jsonA = await fetchWikidataEntities(aBatch, 'labels', `${lang}|en`);
        const entsA = jsonA?.entities && typeof jsonA.entities === 'object' ? jsonA.entities : {};
        for (const aid of aBatch) {
          const label = pickLabel(entsA?.[aid]?.labels, [lang, 'en']);
          if (label) edAuthorLabels[aid] = label;
        }
      }

      for (const [workId, editionOf] of workToEditionOf.entries()) {
        // Only fill if we still haven't resolved this work's author.
        if (out[workId]) continue;
        const str = editionToAuthorString.get(editionOf);
        if (str) {
          out[workId] = str;
          workAuthorCache.set(workId, { expiresAt: now + WORK_AUTHOR_CACHE_TTL_MS, author: str });
          continue;
        }
        const authorQid = editionToAuthorQid.get(editionOf);
        const label = authorQid ? edAuthorLabels[authorQid] : null;
        if (label) {
          out[workId] = label;
          workAuthorCache.set(workId, { expiresAt: now + WORK_AUTHOR_CACHE_TTL_MS, author: label });
        } else {
          workAuthorCache.set(workId, { expiresAt: now + 6 * 60 * 60 * 1000, author: null });
        }
      }
    }
  }

  return out;
};

const buildOpenLibraryCoverUrl = (coverId: number) => `https://covers.openlibrary.org/b/id/${coverId}-L.jpg`;

const pickBestOlDoc = (
  docs: OpenLibrarySearchDoc[],
  title: string,
  author?: string | null,
  opts?: { requireCover?: boolean; requireAuthor?: boolean }
) => {
  const wantTitle = normalizeForLookup(title);
  const wantAuthor = normalizeForLookup(author || '');
  const authorKnown = Boolean(wantAuthor && wantAuthor !== 'unknown');
  const titleTokens = wantTitle.split(' ').filter(Boolean);
  const requireCover = Boolean(opts?.requireCover);
  const requireAuthor = Boolean(opts?.requireAuthor);

  let best: { doc: OpenLibrarySearchDoc; score: number; bonus: number } | null = null;

  for (const d of docs) {
    const dt = typeof d?.title === 'string' ? d.title : '';
    const da = Array.isArray(d?.author_name) ? d.author_name.join(' ') : '';
    const coverId = typeof d?.cover_i === 'number' ? d.cover_i : null;
    const hasAuthor = Array.isArray(d?.author_name) && d.author_name.length > 0;
    // Require *something* useful (author and/or cover). Title-only matches are too risky.
    if (!dt || (!coverId && !hasAuthor)) continue;
    if (requireCover && !coverId) continue;
    if (requireAuthor && !hasAuthor) continue;

    const haveTitle = normalizeForLookup(dt);
    const haveAuthor = normalizeForLookup(da);

    let score = 0;

    if (haveTitle === wantTitle) score += 10;
    else if (haveTitle.includes(wantTitle) || wantTitle.includes(haveTitle)) score += 6;
    else {
      // Weak match: share most tokens
      const wantTokens = new Set(wantTitle.split(' ').filter(Boolean));
      const haveTokens = new Set(haveTitle.split(' ').filter(Boolean));
      const overlap = Array.from(wantTokens).filter((t) => haveTokens.has(t)).length;
      score += Math.min(4, overlap);
    }

    if (authorKnown) {
      if (haveAuthor.includes(wantAuthor) || wantAuthor.includes(haveAuthor)) score += 5;
      else {
        const wantTokens = new Set(wantAuthor.split(' ').filter(Boolean));
        const haveTokens = new Set(haveAuthor.split(' ').filter(Boolean));
        const overlap = Array.from(wantTokens).filter((t) => haveTokens.has(t)).length;
        score += Math.min(3, overlap);
      }
    }

    // Tie-breaker only (do NOT affect threshold): prefer entries with better metadata.
    const bonus = (coverId ? 1 : 0) + (hasAuthor ? 1 : 0);

    // Prefer results with more confident title match; break ties with quality bonus.
    if (!best || score > best.score || (score === best.score && bonus > best.bonus)) best = { doc: d, score, bonus };
  }

  // Require a minimum score to avoid wrong covers.
  // If we *don't* know the author (common for Wikisource), be stricter to avoid incorrect covers.
  // - Very short titles are too ambiguous: require exact title match
  // - Otherwise, require a strong title score
  const threshold = authorKnown ? 8 : titleTokens.length <= 3 ? 10 : 8;
  if (!best || best.score < threshold) return null;
  return best.doc;
};

const normalizeOlAuthor = (value: unknown): string | null => {
  const t = typeof value === 'string' ? value.trim() : '';
  if (!t) return null;
  if (t.toLowerCase() === 'unknown') return null;
  return t;
};

const fetchOpenLibraryMatch = async (
  title: string,
  author?: string | null
): Promise<{ coverUrl: string | null; author: string | null; title: string | null }> => {
  const url = new URL('https://openlibrary.org/search.json');
  url.searchParams.set('title', title);
  if (author && author.trim() && author !== 'Unknown') url.searchParams.set('author', author);
  url.searchParams.set('limit', '10');
  url.searchParams.set('fields', 'title,author_name,cover_i');

  const resp = await withTimeout(
    url.toString(),
    {
      headers: {
        accept: 'application/json',
        'user-agent': OPEN_LIBRARY_UA,
      },
    },
    OPEN_LIBRARY_TIMEOUT_MS
  );

  if (!resp.ok) return { coverUrl: null, author: null, title: null };
  const json = (await resp.json()) as OpenLibrarySearchResponse;
  const docs = Array.isArray(json?.docs) ? json.docs : [];
  const bestCover = pickBestOlDoc(docs, title, author, { requireCover: true });
  const bestAuthorDoc = pickBestOlDoc(docs, title, author, { requireAuthor: true });
  const bestTitleDoc = pickBestOlDoc(docs, title, author, { requireAuthor: true });
  const coverId = typeof bestCover?.cover_i === 'number' ? bestCover.cover_i : null;
  const coverUrl = coverId ? buildOpenLibraryCoverUrl(coverId) : null;
  const bestAuthor = Array.isArray(bestAuthorDoc?.author_name) ? normalizeOlAuthor(bestAuthorDoc?.author_name?.[0]) : null;
  const bestTitleRaw = typeof bestTitleDoc?.title === 'string' ? bestTitleDoc.title : null;
  // Ensure consistent UI titles (no subtitles after ":") regardless of source.
  const bestTitle = bestTitleRaw ? normalizeMainTitle(bestTitleRaw) : null;
  return { coverUrl, author: bestAuthor, title: bestTitle };
};

const getBetterOlMatch = async (
  lang: string,
  title: string,
  author?: string | null
): Promise<{ coverUrl: string | null; author: string | null; title: string | null }> => {
  const t = normalizeMainTitle(title);
  const key = `${lang}::${normalizeForLookup(t)}::${normalizeForLookup(author || '')}`;
  const now = Date.now();
  const cached = olCache.get(key);
  if (cached && cached.expiresAt > now) return { coverUrl: cached.coverUrl, author: cached.author, title: cached.title };

  const inflight = olInFlight.get(key);
  if (inflight) return inflight;

  const p = (async () => {
    try {
      const match = await fetchOpenLibraryMatch(t, author);
      olCache.set(key, {
        expiresAt: now + COVER_CACHE_TTL_MS,
        coverUrl: match.coverUrl || null,
        author: match.author || null,
        title: match.title || null,
      });
      return match;
    } catch {
      olCache.set(key, { expiresAt: now + 6 * 60 * 60 * 1000, coverUrl: null, author: null, title: null }); // short negative cache
      return { coverUrl: null, author: null, title: null };
    } finally {
      olInFlight.delete(key);
    }
  })();

  olInFlight.set(key, p);
  return p;
};

const getBetterCoverUrl = async (lang: string, title: string, author?: string | null): Promise<string | null> => {
  const match = await getBetterOlMatch(lang, title, author);
  return match.coverUrl;
};

const mapWithConcurrency = async <T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> => {
  const results: R[] = new Array(items.length) as R[];
  let next = 0;

  const workers = Array.from({ length: Math.max(1, concurrency) }).map(async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) break;
      results[i] = await fn(items[i]!);
    }
  });

  await Promise.all(workers);
  return results;
};

const enrichGutenbergTitlesWithOpenLibrary = async (lang: string, books: ApiEbook[]) => {
  // Keep bounded: OpenLibrary is a shared public service; avoid large bursts.
  const MAX_ENRICH = 80;
  if (!Array.isArray(books) || books.length === 0) return;

  const deduped = Array.from(new Map(books.filter(Boolean).map((b) => [b.id, b])).values()).slice(0, MAX_ENRICH);

  await mapWithConcurrency(deduped, 6, async (b) => {
    const match = await getBetterOlMatch(lang, b.title, b.author);
    if (match.title) b.title = match.title;
    return true;
  });
};

const enrichCoversForSelection = async (
  lang: string,
  merged: ApiEbook[],
  opts: { maxCategories: number; perCategory: number; trendingLimit: number }
) => {
  // Only enrich for items that will actually be shown (trending + top of category shelves)
  const byCategory = groupByCategory(merged);
  const categories = pickTopCategories(byCategory, opts.maxCategories, opts.perCategory);

  const selected: ApiEbook[] = [];
  const seenIds = new Set<string>();
  const push = (b?: ApiEbook) => {
    if (!b) return;
    if (seenIds.has(b.id)) return;
    seenIds.add(b.id);
    selected.push(b);
  };

  merged.slice(0, opts.trendingLimit).forEach(push);

  const perCatVisible = Math.min(4, opts.perCategory);
  for (const c of categories) {
    (byCategory[c] || []).slice(0, perCatVisible).forEach(push);
  }

  // Enrich at most N items per request to keep latency reasonable (cache fills over time).
  const MAX_ENRICH = 80;
  const isUnknownAuthor = (name?: string | null) => {
    const t = String(name || '').trim();
    if (!t) return true;
    return t.toLowerCase() === 'unknown';
  };
  const candidates = selected
    .filter((b) => b._source !== 'standardebooks')
    .filter((b) => {
      const needsCover = !b.coverUrl || isProbablyGenericGutenbergCover(b.coverUrl) || b._source === 'wikisource';
      const needsAuthor = isUnknownAuthor(b.author);
      // Gutenberg titles are occasionally messy; prefer OpenLibrary's canonical work title when available.
      const needsTitle = b._source === 'gutendex';
      return needsCover || needsAuthor || needsTitle;
    })
    .slice(0, MAX_ENRICH);

  await mapWithConcurrency(candidates, 6, async (b) => {
    const needsAuthor = isUnknownAuthor(b.author);
    const match = await getBetterOlMatch(lang, b.title, b.author);
    if (match.coverUrl) b.coverUrl = match.coverUrl;
    if (needsAuthor && match.author) b.author = match.author;
    if (b._source === 'gutendex' && match.title) b.title = match.title;
    return true;
  });
};

const titleLooksAcademic = (title: string) => {
  const t = title.toLowerCase();
  const blocked = [
    'thesis',
    'dissertation',
    'proceedings',
    'transactions',
    'journal',
    'conference',
    'research',
    'report',
    'technical',
    'laboratory',
    'laboratories',
    'handbook',
    'manual',
    'treatise',
    'monograph',
    'lectures on',
    'notes on',
    'introduction to',
    'textbook',
    'course',
    'syllabus',
  ];
  return blocked.some((k) => t.includes(k));
};

const subjectsLookAcademic = (subjects: string[]) => {
  const hay = normalizeApostrophes(subjects.join(' • ')).toLowerCase();
  const blocked = [
    'theses',
    'dissertations',
    'periodicals',
    'journals',
    'proceedings',
    'transactions',
    'conference',
    'research',
    'statistics',
    'mathematics',
    'physics',
    'chemistry',
    'engineering',
    'medicine',
    'surgery',
    'anatomy',
    'pathology',
    'botany',
    'zoology',
    'geology',
    'astronomy',
    'laboratory',
    'textbooks',
  ];
  return blocked.some((k) => hay.includes(k));
};

const guessCategory = (title: string, subjects: string[]) => {
  const t = title.toLowerCase();
  const s = subjects.map((x) => normalizeApostrophes(x).toLowerCase());
  const hay = `${t} ${s.join(' ')}`;

  const hasAny = (keywords: string[]) => keywords.some((k) => hay.includes(k));

  if (hasAny(["children's", 'childrens', 'juvenile', 'juvenile fiction'])) return "Children's";
  if (hasAny(['romance', 'love story', 'love stories', 'courtship', 'marriage'])) return 'Romance';
  if (hasAny(['mystery', 'detective', 'crime', 'whodunit'])) return 'Mystery';
  if (hasAny(['science fiction', 'sci-fi', 'space travel', 'spaceflight'])) return 'Science Fiction';
  if (hasAny(['fantasy', 'magic', 'wizard', 'dragons'])) return 'Fantasy';
  if (hasAny(['horror', 'ghost story', 'ghost stories', 'gothic', 'supernatural'])) return 'Horror';
  if (hasAny(['comedy', 'humor', 'humour', 'satire'])) return 'Comedy';
  if (hasAny(['drama', 'plays', 'tragedy'])) return 'Drama';
  if (hasAny(['western story', 'western stories', 'frontier', 'cowboy'])) return 'Western';
  if (hasAny(['short story', 'short stories', 'tales', 'stories'])) return 'Short Stories';
  if (hasAny(['myth', 'mythology', 'legend', 'legends', 'folklore', 'fairy tale', 'fairy tales'])) return 'Mythic';
  if (hasAny(['adventure', 'treasure', 'sea story', 'sea stories', 'pirate', 'exploration'])) return 'Adventure';
  if (hasAny(['travel', 'voyage', 'voyages', 'journey', 'journeys', 'tour', 'travels'])) return 'Travel';
  if (hasAny(['poetry', 'poems'])) return 'Poetry';

  // Broad fallbacks
  if (hasAny(['biography', 'autobiography', 'memoir', 'memoirs'])) return 'Biography';
  if (hasAny(['history', 'historical'])) return 'History';
  if (hasAny(['nonfiction', 'essays'])) return 'Nonfiction';
  return 'Fiction';
};

const normalizeAuthorName = (name: string) => {
  const trimmed = (name || '').trim();
  if (!trimmed) return 'Unknown';
  return trimmed;
};

const normalizeTitle = (title: string) => decodeXmlEntities((title || '').trim());

/**
 * Titles across sources frequently include subtitles and MARC-ish subfield codes:
 * - "Main Title: Subtitle"
 * - "Main Title : $b Subtitle"
 *
 * For the Library UI we keep only the main title before ":" (and strip any "$x" fragments).
 */
const normalizeMainTitle = (title: string) => {
  const base = normalizeTitle(title);
  if (!base) return '';

  // Defensive: strip MARC subfield fragments (e.g. "$b Roman")
  const withoutMarc = base.replace(/\s*\$[a-z]\b.*$/i, '');

  // Keep the main title before the first ":" (subtitle delimiter)
  const colonIdx = withoutMarc.indexOf(':');
  const beforeColon = colonIdx >= 0 ? withoutMarc.slice(0, colonIdx) : withoutMarc;

  return beforeColon.replace(/\s+/g, ' ').trim();
};

const dedupeKey = (b: ApiEbook) => `${b.language}::${b.title.toLowerCase()}::${b.author.toLowerCase()}`;

const sleepMs = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, ms)));

type GutendexBook = {
  id: number;
  title: string;
  authors: Array<{ name: string }>;
  subjects: string[];
  bookshelves: string[];
  languages: string[];
  formats: Record<string, string>;
  download_count: number;
  media_type?: string;
};

const fetchGutendex = async (
  lang: string,
  limit: number,
  opts?: { maxPages?: number; pageDelayMs?: number; maxRetries?: number }
): Promise<ApiEbook[]> => {
  const out: ApiEbook[] = [];
  const seen = new Set<string>();
  let page = 1;

  const maxPagesCap =
    typeof opts?.maxPages === 'number' && Number.isFinite(opts.maxPages) ? Math.max(1, Math.floor(opts.maxPages)) : 40;
  const maxPages = Math.min(maxPagesCap, Math.max(6, Math.ceil(limit / 18) + 2));
  const pageDelayMs =
    typeof opts?.pageDelayMs === 'number' && Number.isFinite(opts.pageDelayMs) ? Math.max(0, Math.floor(opts.pageDelayMs)) : 0;
  const maxRetries =
    typeof opts?.maxRetries === 'number' && Number.isFinite(opts.maxRetries) ? Math.max(0, Math.floor(opts.maxRetries)) : 3;

  while (out.length < limit && page <= maxPages) {
    const url = new URL('https://gutendex.com/books/');
    url.searchParams.set('languages', lang);
    url.searchParams.set('sort', 'popular');
    url.searchParams.set('page', String(page));

    const fetchOnce = async () =>
      await fetch(url.toString(), {
        headers: {
          accept: 'application/json',
          'user-agent': 'PolyglotReader/1.0 (catalog-sync)',
        },
      });

    let resp: globalThis.Response | null = null;
    let stop = false;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      resp = await fetchOnce();

      // Gutendex returns 404 when the requested page is beyond the last page.
      if (resp.status === 404) {
        stop = true;
        break;
      }

      if (resp.status === 429) {
        if (attempt >= maxRetries) {
          // Rate limited; keep partial results and stop early for this run.
          stop = true;
          break;
        }

        const retryAfter = resp.headers.get('retry-after');
        const retryAfterSeconds = retryAfter ? parseInt(retryAfter, 10) : NaN;
        const backoff = Number.isFinite(retryAfterSeconds)
          ? Math.max(1, retryAfterSeconds) * 1000
          : Math.min(8000, 800 * Math.pow(2, attempt));

        await sleepMs(backoff + Math.floor(Math.random() * 200));
        continue;
      }

    if (!resp.ok) {
        console.warn(`[gutendex] request failed (HTTP ${resp.status}); stopping early`);
        stop = true;
    }
      break;
    }

    if (stop || !resp || !resp.ok) break;

    const json = (await resp.json()) as { results?: GutendexBook[] };
    const results = Array.isArray(json.results) ? json.results : [];

    for (const r of results) {
      if (out.length >= limit) break;
      if (!r || typeof r.title !== 'string') continue;
      // Keep only text ebooks (exclude audio/image media types)
      if (r.media_type && String(r.media_type).toLowerCase() !== 'text') continue;

      const title = normalizeTitle(r.title);
      const author = normalizeAuthorName(r.authors?.[0]?.name || 'Unknown');
      const subjects = uniq([...(r.subjects || []), ...(r.bookshelves || [])].filter(Boolean).map(normalizeApostrophes));

      if (titleLooksAcademic(title) || subjectsLookAcademic(subjects)) continue;

      // Prefer EPUB (best for future in-app import/reading).
      const downloadUrl = r.formats?.['application/epub+zip'] || null;
      if (!downloadUrl) continue;

      const coverUrl = r.formats?.['image/jpeg'] || null;
      const category = guessCategory(title, subjects);

      const b: ApiEbook = {
        id: `gutenberg:${r.id}`,
        title,
        author,
        // We query Gutendex with `languages=lang`, so enforce the selected language here.
        language: lang,
        category,
        coverUrl,
        downloadUrl,
        _source: 'gutendex',
        _popularity: typeof r.download_count === 'number' ? r.download_count : undefined,
      };

      const key = dedupeKey(b);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(b);
    }

    page += 1;
    if (pageDelayMs > 0) await sleepMs(pageDelayMs);
  }

  return out;
};

type WolneLekturyListItem = {
  title?: string;
  author?: string;
  slug?: string;
  epoch?: string;
  genre?: string;
  kind?: string;
  cover?: string; // relative (e.g. "book/cover/pan-tadeusz.jpg")
  cover_thumb?: string;
};

const WL_BASE = 'https://wolnelektury.pl';
const WL_MEDIA = 'https://wolnelektury.pl/media/';

const wlCoverUrl = (rel?: string | null) => {
  const v = (rel || '').trim();
  if (!v) return null;
  if (/^https?:\/\//i.test(v)) return v;
  return `${WL_MEDIA}${v.replace(/^\/+/, '')}`;
};

const wlEpubUrl = (slug: string) => `${WL_MEDIA}book/epub/${encodeURIComponent(slug)}.epub`;

const fetchWolneLektury = async (limit: number): Promise<ApiEbook[]> => {
  // Polish-only public domain library with direct EPUB downloads.
  const resp = await fetch(`${WL_BASE}/api/books/`, { headers: { accept: 'application/json' } });
  if (!resp.ok) throw new Error(`Wolne Lektury request failed: ${resp.status}`);
  const json = (await resp.json()) as WolneLekturyListItem[];
  const list = Array.isArray(json) ? json : [];

  const out: ApiEbook[] = [];
  const seen = new Set<string>();
  for (const it of list) {
    if (out.length >= limit) break;
    const slug = typeof it?.slug === 'string' ? it.slug.trim() : '';
    const title = normalizeTitle(typeof it?.title === 'string' ? it.title : '');
    const author = normalizeAuthorName(typeof it?.author === 'string' ? it.author : 'Unknown');
    if (!slug || !title) continue;

    const subjects = uniq(
      [it?.epoch, it?.genre, it?.kind]
        .filter((v): v is string => typeof v === 'string' && v.trim().length > 0)
        .map(normalizeApostrophes)
    );
    if (titleLooksAcademic(title) || subjectsLookAcademic(subjects)) continue;

    const category = guessCategory(title, subjects);
    const b: ApiEbook = {
      id: `wolnelektury:${slug}`,
      title,
      author,
      language: 'pl',
      category,
      coverUrl: wlCoverUrl(it?.cover_thumb) || wlCoverUrl(it?.cover) || null,
      downloadUrl: wlEpubUrl(slug),
      _source: 'wolnelektury',
    };

    const key = dedupeKey(b);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(b);
  }

  return out;
};

type WikimediaTopResponse = {
  items?: Array<{
    project: string;
    year: string;
    month: string;
    day: string;
    articles: Array<{ article: string; views: number; rank: number }>;
  }>;
};

type WikisourcePage = {
  pageid?: number;
  title?: string;
  fullurl?: string;
  length?: number;
  missing?: string;
  thumbnail?: { source: string; width?: number; height?: number };
  categories?: Array<{ title: string }>;
  pageprops?: { wikibase_item?: string };
};

type WikisourceQueryResponse = { query?: { pages?: Record<string, WikisourcePage> } };

const isLikelyNonContentPageTitle = (raw: string, opts?: { allowColon?: boolean }) => {
  const title = (raw || '').trim();
  if (!title) return true;
  const lower = title.toLowerCase();

  // Skip namespaces/specials (we want works).
  if (!opts?.allowColon && title.includes(':')) return true;

  // Skip known portal pages.
  const blockedExact = new Set([
    'main page',
    'main_page',
    'accueil',
    'portada',
    'hauptseite',
    'главная страница',
  ]);
  if (blockedExact.has(lower)) return true;

  // Skip very short / navigation-ish titles.
  if (lower === 'home' || lower === 'contents') return true;

  return false;
};

const fetchWikisourceTopTitles = async (lang: string, limit: number): Promise<string[]> => {
  const project = `${lang}.wikisource.org`;

  // Pageviews "top" can lag for the current UTC day; try yesterday, then 2 days ago.
  const tryDays = [1, 2, 3];
  for (const daysAgo of tryDays) {
    const d = utcDaysAgo(daysAgo);
    const { yyyy, mm, dd } = toUtcDateParts(d);
    const url = `https://wikimedia.org/api/rest_v1/metrics/pageviews/top/${project}/all-access/${yyyy}/${mm}/${dd}`;
    const resp = await fetch(url, {
      headers: { accept: 'application/json', 'user-agent': 'PolyglotReader/1.0 (+https://polyglot.local)' },
    });
    if (!resp.ok) continue;
    const json = (await resp.json()) as WikimediaTopResponse;
    const articles = json?.items?.[0]?.articles ?? [];
    if (!Array.isArray(articles) || articles.length === 0) continue;

    const out: string[] = [];
    for (const a of articles) {
      const key = typeof a?.article === 'string' ? a.article : '';
      const decoded = decodeDbKey(key);
      if (isLikelyNonContentPageTitle(decoded)) continue;
      out.push(decoded);
      if (out.length >= limit) break;
    }
    if (out.length) return out;
  }

  return [];
};

type WikisourceAllPagesResponse = {
  continue?: { apcontinue?: string };
  query?: { allpages?: Array<{ title?: string }> };
};

// High-yield crawl: list all pages in main namespace (works) and then filter by our heuristics.
// Useful for smaller-language Wikisources where "top pageviews" returns too few results (e.g., bn, ru).
const fetchWikisourceAllPageTitles = async (lang: string, limit: number): Promise<string[]> => {
  const out: string[] = [];
  let apcontinue: string | null = null;
  const maxLoops = Math.min(80, Math.max(4, Math.ceil(limit / 450) + 4));

  for (let loop = 0; loop < maxLoops && out.length < limit; loop++) {
    const url = new URL(`https://${lang}.wikisource.org/w/api.php`);
    url.searchParams.set('action', 'query');
    url.searchParams.set('format', 'json');
    url.searchParams.set('list', 'allpages');
    url.searchParams.set('apnamespace', '0');
    url.searchParams.set('apfilterredir', 'nonredirects');
    url.searchParams.set('aplimit', '500');
    if (apcontinue) url.searchParams.set('apcontinue', apcontinue);

    const resp = await withTimeout(
      url.toString(),
      { headers: { accept: 'application/json', 'user-agent': 'PolyglotReader/1.0 (+https://polyglot.local)' } },
      6000
    );
    if (!resp.ok) break;
    const json = (await resp.json()) as WikisourceAllPagesResponse;
    const pages = Array.isArray(json?.query?.allpages) ? json.query.allpages : [];
    for (const p of pages) {
      const title = typeof p?.title === 'string' ? p.title.trim() : '';
      if (!title) continue;
      // apnamespace=0 already avoids most non-work namespaces; allow ':' inside titles (subtitles).
      if (isLikelyNonContentPageTitle(title, { allowColon: true })) continue;
      out.push(title);
      if (out.length >= limit) break;
    }

    apcontinue = typeof json?.continue?.apcontinue === 'string' ? json.continue.apcontinue : null;
    if (!apcontinue) break;

    // Be polite to MediaWiki API when crawling lots of pages.
    await sleepMs(150);
  }

  return out;
};

const fetchWikisourcePages = async (lang: string, titles: string[]): Promise<WikisourcePage[]> => {
  if (!titles.length) return [];

  const out: WikisourcePage[] = [];
  const batchSize = 40;

  for (let i = 0; i < titles.length; i += batchSize) {
    const batch = titles.slice(i, i + batchSize);
    const url = new URL(`https://${lang}.wikisource.org/w/api.php`);
    url.searchParams.set('action', 'query');
    url.searchParams.set('format', 'json');
    url.searchParams.set('redirects', '1');
    url.searchParams.set('prop', 'info|pageimages|categories|pageprops');
    url.searchParams.set('inprop', 'url');
    url.searchParams.set('piprop', 'thumbnail');
    url.searchParams.set('pithumbsize', '400');
    url.searchParams.set('cllimit', '20');
    url.searchParams.set('ppprop', 'wikibase_item');
    url.searchParams.set('titles', batch.join('|'));

    const resp = await fetch(url.toString(), {
      headers: { accept: 'application/json', 'user-agent': 'PolyglotReader/1.0 (+https://polyglot.local)' },
    });
    if (!resp.ok) continue;
    const json = (await resp.json()) as WikisourceQueryResponse;
    const pages = json?.query?.pages ?? {};
    for (const p of Object.values(pages)) {
      if (!p || p.missing) continue;
      out.push(p);
    }
  }

  return out;
};

const fetchWikisourceFromTitles = async (lang: string, titles: string[], limit: number): Promise<ApiEbook[]> => {
  const uniqTitles = Array.from(new Set((titles || []).map((t) => String(t || '').trim()).filter(Boolean)));
  if (!uniqTitles.length) return [];

  const pages = await fetchWikisourcePages(lang, uniqTitles);
  const workQids = pages
    .map((p) => p?.pageprops?.wikibase_item)
    .filter((v): v is string => typeof v === 'string' && v.trim().length > 0);
  const authorByWorkQid = await getWorkAuthorsFromWikidata(workQids, lang);

  // Extra fallback for the remaining few: parse wikicode for explicit author links/params.
  const needWikitext: string[] = [];
  for (const p of pages) {
    const title = (p.title || '').trim();
    if (!title || isLikelyNonContentPageTitle(title, { allowColon: true })) continue;

    const length = typeof p.length === 'number' ? p.length : 0;
    if (length > 0 && length < 2200) continue;

    const categories = (p.categories || [])
      .map((c) => (c?.title || '').replace(/^Category:/i, '').trim())
      .filter(Boolean)
      .map(normalizeApostrophes);

    if (titleLooksAcademic(title) || subjectsLookAcademic(categories)) continue;

    const qid = typeof p?.pageprops?.wikibase_item === 'string' ? p.pageprops.wikibase_item.trim() : '';
    const resolvedAuthor = qid && authorByWorkQid[qid] ? authorByWorkQid[qid] : null;
    const categoryAuthor = authorFromWikisourceCategories(categories);
    if (!resolvedAuthor && !categoryAuthor) needWikitext.push(title);
  }
  const wikitextAuthors = await fetchWikisourceWikitextAuthors(lang, needWikitext.slice(0, 40));

  const out: ApiEbook[] = [];
  const seen = new Set<string>();

  for (const p of pages) {
    if (out.length >= limit) break;
    const title = (p.title || '').trim();
    if (!title || isLikelyNonContentPageTitle(title, { allowColon: true })) continue;

    // Page length heuristic: avoid tiny / navigational pages.
    const length = typeof p.length === 'number' ? p.length : 0;
    if (length > 0 && length < 2200) continue;

    const categories = (p.categories || [])
      .map((c) => (c?.title || '').replace(/^Category:/i, '').trim())
      .filter(Boolean)
      .map(normalizeApostrophes);

    if (titleLooksAcademic(title) || subjectsLookAcademic(categories)) continue;

    const category = guessCategory(title, categories);
    const coverUrl = p.thumbnail?.source || null;
    const downloadUrl = buildWikisourceExportEpubUrl(lang, title); // direct EPUB download
    const qid = typeof p?.pageprops?.wikibase_item === 'string' ? p.pageprops.wikibase_item.trim() : '';
    const resolvedAuthor = qid && authorByWorkQid[qid] ? authorByWorkQid[qid] : null;
    const categoryAuthor = authorFromWikisourceCategories(categories);
    const wikitextAuthor = wikitextAuthors[title] || null;

    const b: ApiEbook = {
      id: p.pageid ? `wikisource:${lang}:${p.pageid}` : `wikisource:${lang}:${title.toLowerCase()}`,
      title: normalizeTitle(title),
      author: normalizeAuthorName(categoryAuthor || resolvedAuthor || wikitextAuthor || 'Unknown'),
      language: lang,
      category,
      coverUrl,
      downloadUrl,
      _source: 'wikisource',
    };

    const key = dedupeKey(b);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(b);
  }

  return out;
};

const fetchWikisource = async (lang: string, limit: number): Promise<ApiEbook[]> => {
  const titles = await fetchWikisourceTopTitles(lang, Math.min(400, Math.max(80, limit * 5)));
  return await fetchWikisourceFromTitles(lang, titles, limit);
};

const fetchWikisourceAllPages = async (lang: string, limit: number): Promise<ApiEbook[]> => {
  const titles = await fetchWikisourceAllPageTitles(lang, Math.min(6500, Math.max(800, limit * 8)));
  return await fetchWikisourceFromTitles(lang, titles, limit);
};

const extractFirst = (input: string, re: RegExp): string | null => {
  const m = input.match(re);
  if (!m) return null;
  return typeof m[1] === 'string' ? m[1] : null;
};

const parseXmlAttrs = (tag: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const raw = String(tag || '');
  for (const m of raw.matchAll(/([\w:.-]+)\s*=\s*"([^"]*)"/g)) {
    const k = String(m[1] || '').toLowerCase();
    if (!k) continue;
    out[k] = String(m[2] || '');
  }
  for (const m of raw.matchAll(/([\w:.-]+)\s*=\s*'([^']*)'/g)) {
    const k = String(m[1] || '').toLowerCase();
    if (!k) continue;
    if (out[k] == null) out[k] = String(m[2] || '');
  }
  return out;
};

const pickStandardEbooksEpubLink = (entryXml: string): string | null => {
  const links = Array.from(String(entryXml || '').matchAll(/<link\b[^>]*>/gi)).map((m) => String(m[0] || ''));
  let bestHref: string | null = null;
  let bestScore = -1;
  for (const tag of links) {
    const attrs = parseXmlAttrs(tag);
    const href = (attrs.href || '').trim();
    const type = String(attrs.type || '').toLowerCase();
    if (!href) continue;
    if (!type.includes('application/epub+zip')) continue;

    const rel = String(attrs.rel || '').toLowerCase();
    const title = String(attrs.title || '').toLowerCase();

    let score = 0;
    if (title.includes('recommended')) score += 10;
    if (title.includes('compatible')) score += 3;
    if (rel === 'enclosure') score += 2;
    if (rel.includes('acquisition')) score += 1;

    if (score > bestScore) {
      bestScore = score;
      bestHref = href;
    }
  }
  return bestHref;
};

const fetchStandardEbooksNewReleases = async (): Promise<ApiEbook[]> => {
  const resp = await fetch('https://standardebooks.org/feeds/atom/new-releases', {
    headers: { accept: 'application/atom+xml,application/xml;q=0.9,*/*;q=0.8' },
  });
  if (!resp.ok) {
    throw new Error(`Standard Ebooks feed failed: ${resp.status}`);
  }

  const xml = await resp.text();
  const entries = Array.from(xml.matchAll(/<entry>([\s\S]*?)<\/entry>/g)).map((m) => m[1] || '');
  const out: ApiEbook[] = [];

  for (const e of entries) {
    const id = extractFirst(e, /<id>([^<]+)<\/id>/) || '';
    const titleRaw = extractFirst(e, /<title>([\s\S]*?)<\/title>/) || '';
    const authorRaw = extractFirst(e, /<author>[\s\S]*?<name>([\s\S]*?)<\/name>[\s\S]*?<\/author>/) || 'Unknown';
    const thumb = extractFirst(e, /<media:thumbnail[^>]*\surl="([^"]+)"/) || null;

    // Prefer recommended compatible EPUB. Attribute ordering in XML isn't guaranteed, so parse link tags.
    const epub = pickStandardEbooksEpubLink(e);
    if (!epub) continue;

    const categories = Array.from(e.matchAll(/<category[^>]*term="([^"]+)"/g))
      .map((m) => m[1])
      .filter((v): v is string => typeof v === 'string' && v.trim().length > 0);

    const title = normalizeTitle(titleRaw);
    const author = normalizeAuthorName(decodeXmlEntities(authorRaw));

    if (!title || titleLooksAcademic(title) || subjectsLookAcademic(categories)) continue;

    const category = guessCategory(title, categories);

    // Standard Ebooks feed doesn't include explicit language metadata; the public feed is effectively English.
    out.push({
      id: id ? `standardebooks:${id}` : `standardebooks:${title.toLowerCase()}:${author.toLowerCase()}`,
      title,
      author,
      language: 'en',
      category,
      coverUrl: thumb,
      downloadUrl: epub,
      _source: 'standardebooks',
    });
  }

  return out;
};

// ManyBooks OPDS (free/paid mix; direct EPUB links present in title_detail feeds)
const MANYBOOKS_BASE = 'https://manybooks.net/opds';
// Cloudflare may challenge non-browser user agents; use a stable browser UA to keep this reliable.
const MANYBOOKS_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const MANYBOOKS_TIMEOUT_MS = 9000;
// ManyBooks is sometimes protected by Cloudflare and can return 403/429 to server-side fetches.
// Treat this as a transient "unavailable" state and back off for a while to keep our API fast.
const MANYBOOKS_BLOCK_TTL_MS = 60 * 60 * 1000; // 1 hour
const MANYBOOKS_WARN_THROTTLE_MS = 15 * 60 * 1000; // 15 minutes
let manyBooksBlockedUntil = 0;
let manyBooksLastWarnAt = 0;

type ManyBooksGenre = { name: string; href: string };
type ManyBooksCandidate = { detailHref: string; genre?: string };

const stripXmlText = (value: string) =>
  decodeXmlEntities(String(value || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());

const fetchManyBooksXml = async (url: string): Promise<string> => {
  if (Date.now() < manyBooksBlockedUntil) {
    const err = Object.assign(new Error('ManyBooks temporarily disabled due to recent 403/429 responses'), { status: 403 });
    throw err;
  }
  const resp = await withTimeout(
    url,
    {
      headers: {
        accept: 'application/atom+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': MANYBOOKS_UA,
        // Some Cloudflare configurations behave better with a real referer.
        referer: 'https://manybooks.net/',
      },
    },
    MANYBOOKS_TIMEOUT_MS
  );
  const ct = String(resp.headers.get('content-type') || '').toLowerCase();
  const text = await resp.text();
  const head = text.trimStart().slice(0, 240).toLowerCase();
  const looksHtml =
    ct.includes('text/html') ||
    head.startsWith('<!doctype') ||
    head.startsWith('<html') ||
    head.includes('cloudflare') ||
    head.includes('just a moment');

  if (!resp.ok || looksHtml) {
    // Cloudflare blocks often manifest as 403; rate limits can appear as 429.
    // Sometimes challenges return 200 with an HTML page; treat that as blocked too.
    const status = looksHtml ? 403 : resp.status;
    if (status === 403 || status === 429) {
      manyBooksBlockedUntil = Date.now() + MANYBOOKS_BLOCK_TTL_MS;
    }
    const err = Object.assign(new Error(`ManyBooks request failed: ${status}`), { status });
    throw err;
  }

  return text;
};

const parseManyBooksEntries = (xml: string): { entries: string[]; nextHref: string | null } => {
  const entries = Array.from(xml.matchAll(/<entry>([\s\S]*?)<\/entry>/g)).map((m) => m[1] || '');
  const nextHref = extractFirst(xml, /<link[^>]*rel="next"[^>]*href="([^"]+)"/i);
  return { entries, nextHref: nextHref || null };
};

const fetchManyBooksGenres = async (): Promise<ManyBooksGenre[]> => {
  const xml = await fetchManyBooksXml(`${MANYBOOKS_BASE}/genres`);
  const { entries } = parseManyBooksEntries(xml);
  const out: ManyBooksGenre[] = [];
  for (const e of entries) {
    const titleRaw = extractFirst(e, /<title[\s\S]*?>([\s\S]*?)<\/title>/i) || '';
    const name = stripXmlText(titleRaw);
    const href = extractFirst(e, /<link[^>]*href="([^"]+\/opds\/genres\/\d+)"/i) || '';
    if (!name || !href) continue;
    out.push({ name, href });
  }
  return out;
};

const fetchManyBooksGenreCandidates = async (
  genre: ManyBooksGenre,
  opts: { maxPages: number; maxCandidates: number }
): Promise<ManyBooksCandidate[]> => {
  const out: ManyBooksCandidate[] = [];
  let url = `${genre.href}?n=0`;
  for (let page = 0; page < opts.maxPages && out.length < opts.maxCandidates; page++) {
    const xml = await fetchManyBooksXml(url);
    const parsed = parseManyBooksEntries(xml);
    for (const e of parsed.entries) {
      const detailHref =
        extractFirst(e, /href="(https:\/\/manybooks\.net\/opds\/title_detail\/\d+)"/i) ||
        extractFirst(e, /href="([^"]+\/opds\/title_detail\/\d+)"/i) ||
        null;
      if (!detailHref) continue;
      out.push({ detailHref, genre: genre.name });
      if (out.length >= opts.maxCandidates) break;
    }
    if (!parsed.nextHref) break;
    url = parsed.nextHref;
  }
  return out;
};

const parseManyBooksTitleDetail = (xml: string) => {
  const entry = extractFirst(xml, /<entry>([\s\S]*?)<\/entry>/i) || '';
  const titleRaw = extractFirst(entry, /<title>([\s\S]*?)<\/title>/i) || '';
  const authorRaw = extractFirst(entry, /<author>[\s\S]*?<name>([\s\S]*?)<\/name>[\s\S]*?<\/author>/i) || '';
  const cover =
    extractFirst(entry, /<link[^>]*rel="http:\/\/opds-spec\.org\/thumbnail"[^>]*href="([^"]+)"/i) ||
    extractFirst(entry, /<link[^>]*rel="http:\/\/opds-spec\.org\/cover"[^>]*href="([^"]+)"/i) ||
    null;
  const epub =
    extractFirst(entry, /<link[^>]*type="application\/epub\+zip"[^>]*href="([^"]+)"/i) ||
    extractFirst(entry, /<link[^>]*href="([^"]+\.epub)"/i) ||
    null;

  const contentRaw = extractFirst(entry, /<content[^>]*>([\s\S]*?)<\/content>/i) || '';
  const content = String(contentRaw || '');
  const langFromContent =
    extractFirst(content, /Language<\/strong>\s*:\s*([a-zA-Z-]{2,8})/i) ||
    extractFirst(content, /Language\s*:\s*([a-zA-Z-]{2,8})/i) ||
    null;

  return {
    title: stripXmlText(titleRaw),
    author: stripXmlText(authorRaw),
    language: (langFromContent || '').trim().toLowerCase(),
    coverUrl: cover,
    epubUrl: epub,
  };
};

const fetchManyBooks = async (lang: string, limit: number): Promise<ApiEbook[]> => {
  if (Date.now() < manyBooksBlockedUntil) return [];

  const wantLang = normalizeLang(lang) || lang;
  const genres = await fetchManyBooksGenres();

  const preferredGenreNames = [
    'Travel',
    'Adventure',
    'Mystery',
    'Horror',
    'Fantasy',
    'Science Fiction',
    'Romance',
    'Short Stories',
    'Drama',
    'Comedy',
    'Biography',
    'History',
    'Western',
    'Poetry',
    'Fiction',
    'Nonfiction',
  ];

  const genreMap = new Map(genres.map((g) => [normalizeForLookup(g.name), g]));
  const selectedGenres = preferredGenreNames
    .map((n) => genreMap.get(normalizeForLookup(n)))
    .filter((g): g is ManyBooksGenre => Boolean(g))
    .slice(0, 8);

  // If genre matching fails for some reason, fall back to scanning New Titles.
  const candidates: ManyBooksCandidate[] = [];
  const maxCandidates = Math.min(420, Math.max(120, limit * 8));

  if (selectedGenres.length) {
    for (const g of selectedGenres) {
      const part = await fetchManyBooksGenreCandidates(g, { maxPages: 4, maxCandidates: Math.ceil(maxCandidates / selectedGenres.length) });
      candidates.push(...part);
    }
  } else {
    // new_titles provides only title_detail links; same parsing approach
    let url = `${MANYBOOKS_BASE}/new_titles`;
    for (let page = 0; page < 10 && candidates.length < maxCandidates; page++) {
      const xml = await fetchManyBooksXml(url);
      const parsed = parseManyBooksEntries(xml);
      for (const e of parsed.entries) {
        const detailHref =
          extractFirst(e, /href="(https:\/\/manybooks\.net\/opds\/title_detail\/\d+)"/i) ||
          extractFirst(e, /href="([^"]+\/opds\/title_detail\/\d+)"/i) ||
          null;
        if (!detailHref) continue;
        candidates.push({ detailHref });
        if (candidates.length >= maxCandidates) break;
      }
      if (!parsed.nextHref) break;
      url = parsed.nextHref;
    }
  }

  // Deduplicate title_detail URLs (some books show in multiple genres).
  const uniqCandidates: ManyBooksCandidate[] = [];
  const seenDetail = new Set<string>();
  for (const c of candidates) {
    const key = c.detailHref;
    if (seenDetail.has(key)) continue;
    seenDetail.add(key);
    uniqCandidates.push(c);
  }

  const parsed = await mapWithConcurrency(uniqCandidates, 6, async (c) => {
    try {
      const xml = await fetchManyBooksXml(c.detailHref);
      const detail = parseManyBooksTitleDetail(xml);
      const epubUrl = detail.epubUrl;
      if (!epubUrl) return null;

      const mbLang = normalizeLang(detail.language) || detail.language;
      if (!mbLang || mbLang !== wantLang) return null;

      const title = normalizeTitle(detail.title);
      const author = normalizeAuthorName(detail.author || 'Unknown');
      if (!title || !author) return null;

      const subjects = uniq([c.genre].filter((v): v is string => typeof v === 'string' && v.trim().length > 0).map(normalizeApostrophes));
      if (titleLooksAcademic(title) || subjectsLookAcademic(subjects)) return null;

      const category = guessCategory(title, subjects);
      const idNum = extractFirst(c.detailHref, /title_detail\/(\d+)/i) || '';

      const b: ApiEbook = {
        id: idNum ? `manybooks:${idNum}` : `manybooks:${title.toLowerCase()}:${author.toLowerCase()}`,
        title,
        author,
        language: wantLang,
        category,
        coverUrl: detail.coverUrl || null,
        downloadUrl: epubUrl,
        _source: 'manybooks',
      };

      return b;
    } catch {
      return null;
    }
  });

  const out: ApiEbook[] = [];
  const seen = new Set<string>();
  for (const b of parsed) {
    if (!b) continue;
    const key = dedupeKey(b);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(b);
    if (out.length >= limit) break;
  }

  return out;
};

const toTopAuthors = (books: ApiEbook[], limit = 10): ApiTopAuthor[] => {
  const counts = new Map<string, number>();
  for (const b of books) {
    const a = (b.author || '').trim();
    if (!a || a === 'Unknown') continue;
    counts.set(a, (counts.get(a) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
    .slice(0, limit);
};

const groupByCategory = (books: ApiEbook[]) => {
  const by: Record<string, ApiEbook[]> = {};
  for (const b of books) {
    const cat = (b.category || 'Fiction').trim() || 'Fiction';
    if (!by[cat]) by[cat] = [];
    by[cat]!.push(b);
  }
  return by;
};

const sortCategories = (cats: string[]) => {
  const set = new Set(cats);
  const ordered = CATEGORY_ORDER.filter((c) => set.has(c));
  const rest = cats.filter((c) => !CATEGORY_ORDER.includes(c)).sort((a, b) => a.localeCompare(b));
  return [...ordered, ...rest];
};

const pickTopCategories = (byCategory: Record<string, ApiEbook[]>, maxCategories: number, perCategory: number) => {
  const entries = Object.entries(byCategory)
    .map(([category, items]) => ({ category, count: items.length }))
    .filter((e) => e.count > 0);

  const rank = (cat: string) => {
    const idx = CATEGORY_ORDER.indexOf(cat);
    return idx >= 0 ? idx : 9999;
  };

  // Prefer categories with enough books, but keep ordering stable & "human" via rank.
  const minCount = Math.max(3, Math.min(8, Math.floor(perCategory / 2)));
  const primary = entries
    .filter((e) => e.count >= minCount)
    .sort((a, b) => b.count - a.count || rank(a.category) - rank(b.category) || a.category.localeCompare(b.category));
  const fallback = entries
    .filter((e) => e.count < minCount)
    .sort((a, b) => b.count - a.count || rank(a.category) - rank(b.category) || a.category.localeCompare(b.category));

  const selected = [...primary, ...fallback].slice(0, maxCategories).map((e) => e.category);
  return sortCategories(selected);
};

const mergeInterleaved = (lang: string, lists: ApiEbook[][], pool: number) => {
  const out: ApiEbook[] = [];
  const seen = new Set<string>();
  const idx = lists.map(() => 0);

  while (out.length < pool) {
    let progressed = false;

    for (let i = 0; i < lists.length && out.length < pool; i++) {
      const list = lists[i] || [];
      while (idx[i] < list.length) {
        const b = list[idx[i]++]!;
        if (!b) continue;
        if (b.language !== lang) continue;
        const key = dedupeKey(b);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(b);
        progressed = true;
        break;
      }
    }

    if (!progressed) break;
  }

  return out;
};

// Simple in-memory cache (per language)
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = new Map<string, { expiresAt: number; data: ApiEbooksResponse }>();

// Rebuilding author/category counts can be expensive when ingesting in small batches.
// Throttle rebuilds per language to keep the API responsive while still updating categories regularly.
const COUNTS_REBUILD_THROTTLE_MS = 60 * 1000; // 1 minute
const countsRebuildLastAt = new Map<string, number>();

type AggregatedPool = {
  generatedAt: string;
  merged: ApiEbook[];
};

// Cache the aggregated merged pool so multiple endpoints (overview + category pagination)
// can share the same underlying data without refetching external sources.
const poolCache = new Map<string, { expiresAt: number; data: AggregatedPool }>();
const poolInFlight = new Map<string, Promise<AggregatedPool>>();

// --- DB-backed ebook catalog (Approach 2) ---
const EBOOK_CATALOG_ITEMS_TABLE = 'ebook_catalog_items';
const EBOOK_CATALOG_SYNC_STATE_TABLE = 'ebook_catalog_sync_state';
const EBOOK_CATALOG_CATEGORY_COUNTS_TABLE = 'ebook_catalog_category_counts';
const EBOOK_CATALOG_AUTHOR_COUNTS_TABLE = 'ebook_catalog_author_counts';

type EbookCatalogItemRow = {
  id: string;
  lang_code: string;
  title: string;
  author: string;
  category: string;
  cover_url?: string | null;
  download_url: string;
  source: string;
  source_id?: string | null;
  source_popularity?: number | null;
  author_norm: string;
  title_norm: string;
  created_at?: string;
  updated_at?: string;
  last_seen_at?: string;
};

type EbookCatalogSyncStateRow = {
  lang_code: string;
  status: string;
  last_started_at?: string | null;
  last_completed_at?: string | null;
  last_error?: string | null;
  last_items_upserted?: number | null;
  updated_at?: string;
};

const catalogSyncInFlight = new Map<string, Promise<{ upserted: number }>>();

// Catalog normalization must work for non-Latin scripts too (e.g., ru, bn).
// Keep Unicode letters, but normalize whitespace/case and strip combining marks.
const normalizeForCatalogNorm = (value: string) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const noMarks = raw.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  return noMarks.replace(/\s+/g, ' ').trim().toLowerCase();
};

const toCatalogId = (lang: string, title: string, author: string) =>
  `${lang}::${normalizeForCatalogNorm(title)}::${normalizeForCatalogNorm(author)}`;

const sourceRank: Record<EbookSource, number> = {
  standardebooks: 0,
  wolnelektury: 1,
  gutendex: 2,
  wikisource: 3,
  manybooks: 4,
};

const pickBetterCandidate = (a: ApiEbook, b: ApiEbook): ApiEbook => {
  // Prefer: having a cover, higher popularity, then higher-priority source.
  const aHasCover = !!a.coverUrl;
  const bHasCover = !!b.coverUrl;
  if (aHasCover !== bHasCover) return bHasCover ? b : a;

  const ap = typeof a._popularity === 'number' ? a._popularity : 0;
  const bp = typeof b._popularity === 'number' ? b._popularity : 0;
  if (ap !== bp) return bp > ap ? b : a;

  const ar = a._source ? sourceRank[a._source] : 999;
  const br = b._source ? sourceRank[b._source] : 999;
  if (ar !== br) return br < ar ? b : a;

  // Otherwise keep the existing one.
  return a;
};

const upsertCatalogItems = async (rows: EbookCatalogItemRow[]) => {
  const chunkSize = 400;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const resp = await supabase.from(EBOOK_CATALOG_ITEMS_TABLE).upsert(chunk, {
      onConflict: 'id',
      defaultToNull: false,
    });
    if (resp.error) throw resp.error;
  }
};

const upsertCatalogSyncState = async (patch: Partial<EbookCatalogSyncStateRow> & { lang_code: string }) => {
  const nowIso = new Date().toISOString();
  const resp = await supabase.from(EBOOK_CATALOG_SYNC_STATE_TABLE).upsert(
    [
      {
        ...patch,
        updated_at: nowIso,
      } as EbookCatalogSyncStateRow,
    ],
    { onConflict: 'lang_code', defaultToNull: false }
  );
  if (resp.error) throw resp.error;
};

export const syncEbookCatalogLanguage = async (langRaw: string): Promise<{ upserted: number }> => {
  const lang = normalizeLang(langRaw) || langRaw || 'en';
  const existing = catalogSyncInFlight.get(lang);
  if (existing) return await existing;

  const work = (async () => {
    const startedAt = new Date().toISOString();
    await upsertCatalogSyncState({
      lang_code: lang,
      status: 'running',
      last_started_at: startedAt,
      last_error: null,
    });

    try {
      const safeFetch = async (label: string, fn: () => Promise<ApiEbook[]>) => {
        try {
          return await fn();
        } catch (e) {
          console.warn(`[catalog-sync] ${label} unavailable`, e);
          return [];
        }
      };

      // Fetch more than the UI ever requests; DB pagination serves the rest.
      const gutendexLimit = clampInt(process.env.EBOOK_CATALOG_GUTENDEX_LIMIT, 200, 5000, 1200);
      const gutendexMaxPages = clampInt(process.env.EBOOK_CATALOG_GUTENDEX_MAX_PAGES, 10, 400, 80);
      const gutendexPageDelayMs = clampInt(process.env.EBOOK_CATALOG_GUTENDEX_PAGE_DELAY_MS, 0, 2000, 150);
      const wikisourceLimitDefault = clampInt(process.env.EBOOK_CATALOG_WIKISOURCE_LIMIT, 100, 5000, 600);
      const wikisourceAllPagesLimit = clampInt(process.env.EBOOK_CATALOG_WIKISOURCE_ALLPAGES_LIMIT, 200, 12000, 3500);
      const wikisourceLimit = lang === 'ru' || lang === 'bn' ? wikisourceAllPagesLimit : wikisourceLimitDefault;
      // ManyBooks can be disabled by setting this to 0 (useful if the host is blocked by Cloudflare from your server IP).
      const manyBooksLimit = clampInt(process.env.EBOOK_CATALOG_MANYBOOKS_LIMIT, 0, 5000, 350);

      const [standard, wolne, gutendex, wikisource, manybooks] = await Promise.all([
        lang === 'en' ? safeFetch('Standard Ebooks', () => fetchStandardEbooksNewReleases()) : Promise.resolve([]),
        lang === 'pl' ? safeFetch('Wolne Lektury', () => fetchWolneLektury(2500)) : Promise.resolve([]),
        safeFetch('Gutendex', () =>
          fetchGutendex(lang, gutendexLimit, { maxPages: gutendexMaxPages, pageDelayMs: gutendexPageDelayMs, maxRetries: 3 })
        ),
        safeFetch('Wikisource', () => (lang === 'ru' || lang === 'bn' ? fetchWikisourceAllPages(lang, wikisourceLimit) : fetchWikisource(lang, wikisourceLimit))),
        manyBooksLimit > 0 ? safeFetch('ManyBooks', () => fetchManyBooks(lang, manyBooksLimit)) : Promise.resolve([]),
      ]);

      console.log(
        `[catalog-sync] ${lang} fetched: standard=${standard.length} wolne=${wolne.length} gutendex=${gutendex.length} wikisource=${wikisource.length} manybooks=${manybooks.length}`
      );

      // Merge candidates into a single deduped set.
      const deduped = new Map<string, ApiEbook>();
      const all = [...standard, ...wolne, ...gutendex, ...wikisource, ...manybooks];

      for (const b of all) {
        if (!b) continue;
        const title = String(b.title || '').trim();
        const author = String(b.author || '').trim();
        const downloadUrl = String(b.downloadUrl || '').trim();
        if (!title || !author || !downloadUrl) continue;
        if (!hasResolvedAuthor(b)) continue;

        const id = toCatalogId(lang, title, author);
        const prev = deduped.get(id);
        if (!prev) {
          deduped.set(id, b);
        } else {
          deduped.set(id, pickBetterCandidate(prev, b));
        }
      }

      const nowIso = new Date().toISOString();
      const rows: EbookCatalogItemRow[] = [];
      for (const b of deduped.values()) {
        const title = String(b.title || '').trim();
        const author = String(b.author || '').trim();
        const downloadUrl = String(b.downloadUrl || '').trim();
        if (!title || !author || !downloadUrl) continue;
        if (!hasResolvedAuthor(b)) continue;

        const row: EbookCatalogItemRow = {
          id: toCatalogId(lang, title, author),
          lang_code: lang,
          title,
          author,
          category: String(b.category || 'Fiction').trim() || 'Fiction',
          download_url: downloadUrl,
          source: String(b._source || 'gutendex'),
          source_id: b.id,
          source_popularity: typeof b._popularity === 'number' ? Math.max(0, Math.floor(b._popularity)) : null,
          author_norm: normalizeForCatalogNorm(author),
          title_norm: normalizeForCatalogNorm(title),
          updated_at: nowIso,
          last_seen_at: nowIso,
        };
        if (b.coverUrl) row.cover_url = b.coverUrl;
        rows.push(row);
      }

      await upsertCatalogItems(rows);

      // Update aggregates (categories/authors) for fast UI rendering.
      try {
        const rpc = await supabase.rpc('ebook_catalog_rebuild_counts', { p_lang: lang });
        if (rpc.error) throw rpc.error;
      } catch (e) {
        console.warn('[catalog-sync] rebuild_counts failed (non-fatal)', e);
      }

      const completedAt = new Date().toISOString();
      await upsertCatalogSyncState({
        lang_code: lang,
        status: 'idle',
        last_completed_at: completedAt,
        last_items_upserted: rows.length,
        last_error: null,
      });

      return { upserted: rows.length };
    } catch (e) {
      const completedAt = new Date().toISOString();
      await upsertCatalogSyncState({
        lang_code: lang,
        status: 'error',
        last_completed_at: completedAt,
        last_error: (e as any)?.message ? String((e as any).message) : 'Catalog sync failed',
      });
      throw e;
    }
  })();

  catalogSyncInFlight.set(lang, work);
  try {
    return await work;
  } finally {
    catalogSyncInFlight.delete(lang);
  }
};

const hasResolvedAuthor = (b: ApiEbook) => {
  const a = String(b?.author || '').trim();
  return !!a && a.toLowerCase() !== 'unknown';
};

const stripInternalFields = (b: ApiEbook): ApiEbook => {
  // Ensure we never leak `_source`
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { _source, _popularity, ...rest } = b;
  const author = String(rest.author || '').trim();
  const rawTitle = String(rest.title || '').trim();
  const title = normalizeMainTitle(rawTitle);
  return {
    ...rest,
    title,
    // Don't send "Unknown" to the client; the UI should hide the author line when we can't resolve it.
    author: author && author.toLowerCase() !== 'unknown' ? rest.author : '',
  };
};

const buildResponse = (
  lang: string,
  merged: ApiEbook[],
  opts: { maxCategories: number; perCategory: number; trendingLimit: number }
): ApiEbooksResponse => {
  // Exclude items whose author can't be resolved (empty/"Unknown") so the client never sees blank author rows.
  const filtered = merged.filter(hasResolvedAuthor);

  const byCategory = groupByCategory(filtered);
  // Return *all* categories present for this language (not just top N).
  // Note: categories are our normalized buckets (via guessCategory), not the raw library taxonomies.
  const categories = sortCategories(Object.keys(byCategory).filter((c) => (byCategory[c] || []).length > 0));

  return {
    lang,
    generatedAt: new Date().toISOString(),
    categories,
    topAuthors: toTopAuthors(filtered),
    trending: filtered.slice(0, opts.trendingLimit).map(stripInternalFields),
    byCategory: Object.fromEntries(
      categories.map((c) => [(c || 'Fiction'), (byCategory[c] || []).slice(0, opts.perCategory).map(stripInternalFields)])
    ),
  };
};

const fetchAggregatedPool = async (
  lang: string,
  opts: { pool: number; maxCategories: number; perCategory: number; trendingLimit: number }
): Promise<AggregatedPool> => {
  // 1) Gutendex (Project Gutenberg via API wrapper)
  const gutendex = await fetchGutendex(lang, opts.pool);

  // 2) Wikisource (multi-language, pageviews-powered)
  let wikisource: ApiEbook[] = [];
  try {
    // Aim for ~25–35% from Wikisource, but increase if Gutenberg has very little for this language.
    const target =
      gutendex.length < Math.max(12, Math.floor(opts.pool * 0.35))
        ? Math.min(opts.pool, Math.max(120, Math.floor(opts.pool * 0.7)))
        : Math.min(200, Math.max(80, Math.floor(opts.pool * 0.3)));
    wikisource = await fetchWikisource(lang, target);
  } catch (e) {
    console.warn('[ebooks] Wikisource fetch unavailable', e);
    wikisource = [];
  }

  // 3) Standard Ebooks (public new-releases only; full catalog is gated; effectively English)
  let standard: ApiEbook[] = [];
  if (lang === 'en') {
    try {
      standard = await fetchStandardEbooksNewReleases();
    } catch (e) {
      console.warn('[ebooks] Standard Ebooks feed unavailable', e);
      standard = [];
    }
  }

  // 4) ManyBooks (OPDS, multi-language; includes both public domain and contemporary free ebooks)
  let manybooks: ApiEbook[] = [];
  try {
    // Aim for ~20–30% from ManyBooks, but keep the fetch bounded.
    const target = Math.min(220, Math.max(60, Math.floor(opts.pool * 0.25)));
    manybooks = await fetchManyBooks(lang, target);
  } catch (e) {
    const now = Date.now();
    const status = (e as any)?.status;
    if (now - manyBooksLastWarnAt > MANYBOOKS_WARN_THROTTLE_MS) {
      manyBooksLastWarnAt = now;
      if (status === 403 || status === 429) {
        console.warn(`[ebooks] ManyBooks unavailable (HTTP ${status}; backing off temporarily)`);
      } else {
        console.warn('[ebooks] ManyBooks unavailable', e);
      }
    }
    manybooks = [];
  }

  // 4) Wolne Lektury (Polish-only, direct EPUB)
  let wolne: ApiEbook[] = [];
  if (lang === 'pl') {
    try {
      wolne = await fetchWolneLektury(Math.min(opts.pool, 400));
    } catch (e) {
      console.warn('[ebooks] Wolne Lektury unavailable', e);
      wolne = [];
    }
  }

  // Interleave sources so the UI actually shows a mix (not dominated by one library).
  const merged = mergeInterleaved(lang, [standard, gutendex, manybooks, wikisource, wolne], opts.pool);

  // Upgrade generic/missing covers using a dedicated cover index (keeps ebook content from original sources).
  try {
    await enrichCoversForSelection(lang, merged, {
      maxCategories: opts.maxCategories,
      perCategory: opts.perCategory,
      trendingLimit: opts.trendingLimit,
    });
  } catch (e) {
    console.warn('[ebooks] Cover enrichment failed (continuing with fallback covers)', e);
  }

  return { generatedAt: new Date().toISOString(), merged };
};

const getAggregatedPool = async (
  lang: string,
  opts: { pool: number; maxCategories: number; perCategory: number; trendingLimit: number }
): Promise<AggregatedPool> => {
  const key = `${lang}:${opts.pool}`;
  const now = Date.now();
  const cached = poolCache.get(key);
  if (cached && cached.expiresAt > now) return cached.data;

  const inFlight = poolInFlight.get(key);
  if (inFlight) return await inFlight;

  const work = (async () => {
    const data = await fetchAggregatedPool(lang, opts);
    poolCache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, data });
    return data;
  })();

  poolInFlight.set(key, work);
  try {
    return await work;
  } finally {
    poolInFlight.delete(key);
  }
};

const fetchAggregated = async (
  lang: string,
  opts: { pool: number; maxCategories: number; perCategory: number; trendingLimit: number }
): Promise<ApiEbooksResponse> => {
  const pool = await getAggregatedPool(lang, opts);
  const response = buildResponse(lang, pool.merged, opts);
  // Keep timestamps stable across endpoints that share the same cached pool.
  response.generatedAt = pool.generatedAt;
  return response;
};

export const getEbooks = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const perCategory = clampInt(req.query.perCategory, 6, 24, 12);
    const trendingLimit = clampInt(req.query.trendingLimit, 6, 24, 12);
    const maxCategories = clampInt(req.query.maxCategories, 6, 32, 14);

    const cacheKey = `catalog:${lang}:${perCategory}:${trendingLimit}:${maxCategories}`;
    const now = Date.now();
    const cached = cache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      res.setHeader('Cache-Control', 'private, max-age=300');
      res.json(cached.data);
      return;
    }

    const isMissingCatalogError = (e: any) => {
      const msg = String(e?.message || '');
      return msg.includes('ebook_catalog_') || msg.includes('relation') || msg.includes('does not exist');
    };

    const gutenbergTitleCandidates: ApiEbook[] = [];

    const toApiFromRow = (r: any): ApiEbook => {
      const id = String(r?.id || '');
      const rawTitle = String(r?.title || '');
      const source = String(r?.source || '');
      const sourceId = String(r?.source_id || '');
      const isGutenberg = source === 'gutendex' || sourceId.startsWith('gutenberg:');
      const title = normalizeMainTitle(rawTitle);
      const book: ApiEbook = {
        id,
        title,
        author: String(r?.author || ''),
        language: String(r?.lang_code || lang),
        category: String(r?.category || 'Fiction'),
        coverUrl: r?.cover_url ?? null,
        downloadUrl: r?.download_url ?? null,
      };
      if (isGutenberg) gutenbergTitleCandidates.push(book);
      return book;
    };

    try {
      // Prefer sync timestamp if available.
      const syncResp = await supabase
        .from(EBOOK_CATALOG_SYNC_STATE_TABLE)
        .select('last_completed_at')
        .eq('lang_code', lang)
        .maybeSingle();
      if (syncResp.error) throw syncResp.error;
      const generatedAt = (syncResp.data as any)?.last_completed_at || new Date().toISOString();

      // Categories (precomputed).
      const catResp = await supabase
        .from(EBOOK_CATALOG_CATEGORY_COUNTS_TABLE)
        .select('category,count')
        .eq('lang_code', lang)
        .order('count', { ascending: false })
        // Fetch a little extra to account for any malformed rows; we'll slice after filtering.
        .limit(Math.max(12, maxCategories * 2));
      if (catResp.error) throw catResp.error;
      const categoriesByCount = (Array.isArray(catResp.data) ? catResp.data : [])
          .filter((r: any) => typeof r?.category === 'string' && r.category.trim() && (r?.count || 0) > 0)
          .map((r: any) => String(r.category).trim())
        .slice(0, maxCategories);
      const categories = sortCategories(categoriesByCount);

      // Top authors (precomputed).
      const authorsResp = await supabase
        .from(EBOOK_CATALOG_AUTHOR_COUNTS_TABLE)
        .select('author,count')
        .eq('lang_code', lang)
        .order('count', { ascending: false })
        .limit(12);
      if (authorsResp.error) throw authorsResp.error;
      const topAuthors: ApiTopAuthor[] = (Array.isArray(authorsResp.data) ? authorsResp.data : [])
        .filter((r: any) => typeof r?.author === 'string' && r.author.trim() && typeof r?.count === 'number')
        .map((r: any) => ({ name: String(r.author).trim(), count: Math.max(0, Math.floor(r.count)) }));

      // Trending (best-effort: popularity desc, then updated_at desc).
      const trendingResp = await supabase
        .from(EBOOK_CATALOG_ITEMS_TABLE)
        .select('id,title,author,lang_code,category,cover_url,download_url,source,source_id')
        .eq('lang_code', lang)
        .neq('download_url', '')
        .order('source_popularity', { ascending: false, nullsFirst: false })
        .order('updated_at', { ascending: false })
        .limit(trendingLimit);
      if (trendingResp.error) throw trendingResp.error;
      const trending = (Array.isArray(trendingResp.data) ? trendingResp.data : []).map(toApiFromRow);

      // Per-category shelf previews.
      const shelves = await mapWithConcurrency(categories, 6, async (category) => {
        const shelfResp = await supabase
          .from(EBOOK_CATALOG_ITEMS_TABLE)
          .select('id,title,author,lang_code,category,cover_url,download_url,source,source_id')
          .eq('lang_code', lang)
          .eq('category', category)
          .neq('download_url', '')
          .order('source_popularity', { ascending: false, nullsFirst: false })
          .order('updated_at', { ascending: false })
          .limit(perCategory);
        if (shelfResp.error) throw shelfResp.error;
        const items = (Array.isArray(shelfResp.data) ? shelfResp.data : []).map(toApiFromRow);
        return [category, items] as const;
      });

      const byCategory = Object.fromEntries(shelves.filter(([, items]) => Array.isArray(items) && items.length > 0));

      // For Gutenberg items, prefer OpenLibrary's canonical title when available (fallback to Gutenberg otherwise).
      await enrichGutenbergTitlesWithOpenLibrary(lang, gutenbergTitleCandidates);

      const data: ApiEbooksResponse = {
        lang,
        generatedAt,
        categories,
        topAuthors,
        trending,
        byCategory,
      };

    cache.set(cacheKey, { expiresAt: now + CACHE_TTL_MS, data });

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.json(data);
      return;
    } catch (e) {
      // If the DB catalog isn't set up yet, return a clear message (instead of a cryptic Supabase error).
      if (isMissingCatalogError(e)) {
        res.status(500).json({ message: 'Ebook catalog is not initialized. Run sql/ebooks_catalog.sql in Supabase first.' });
        return;
      }
      throw e;
    }
  } catch (error) {
    console.error('getEbooks failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

type ApiEbooksCategoryResponse = {
  lang: string;
  category: string;
  generatedAt: string;
  total: number;
  cursor: number;
  limit: number;
  nextCursor: number | null;
  items: ApiEbook[];
};

type ApiEbooksCategoriesResponse = {
  lang: string;
  generatedAt: string;
  cursor: number;
  limit: number;
  nextCursor: number | null;
  items: Array<{ category: string; count: number }>;
};

// Paginated list of categories (with counts). Keeps the Library overview lightweight while still allowing users to browse everything.
export const getEbooksCategories = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const cursor = clampInt((req.query.cursor ?? req.query.offset) as unknown, 0, 9999999, 0);
    const limit = clampInt((req.query.pageSize ?? req.query.limit) as unknown, 10, 200, 60);

    const resp = await supabase
      .from(EBOOK_CATALOG_CATEGORY_COUNTS_TABLE)
      .select('category,count')
      .eq('lang_code', lang)
      .order('count', { ascending: false })
      .range(cursor, cursor + limit - 1);
    if (resp.error) throw resp.error;

    const rows = Array.isArray(resp.data) ? resp.data : [];
    const items = rows
      .filter((r: any) => typeof r?.category === 'string' && r.category.trim() && typeof r?.count === 'number')
      .map((r: any) => ({ category: String(r.category).trim(), count: Math.max(0, Math.floor(r.count)) }));

    const nextCursor = rows.length >= limit ? cursor + rows.length : null;

    const payload: ApiEbooksCategoriesResponse = {
      lang,
      generatedAt: new Date().toISOString(),
      cursor,
      limit,
      nextCursor,
      items,
    };

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.json(payload);
  } catch (error) {
    console.error('getEbooksCategories failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

// Paginated access to all books for a single category (scalable alternative to returning huge arrays).
export const getEbooksCategory = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const categoryRaw = typeof req.query.category === 'string' ? req.query.category.trim() : '';
    if (!categoryRaw) {
      res.status(400).json({ message: 'Missing category' });
      return;
    }

    const cursor = clampInt((req.query.cursor ?? req.query.offset) as unknown, 0, 9999999, 0);
    const limit = clampInt((req.query.pageSize ?? req.query.limit) as unknown, 10, 100, 40);
    const allowEmpty = String(req.query.allowEmpty || '').toLowerCase() === '1' || String(req.query.allowEmpty || '').toLowerCase() === 'true';

    const gutenbergTitleCandidates: ApiEbook[] = [];

    const toApiFromRow = (r: any): ApiEbook => {
      const id = String(r?.id || '');
      const rawTitle = String(r?.title || '');
      const source = String(r?.source || '');
      const sourceId = String(r?.source_id || '');
      const isGutenberg = source === 'gutendex' || sourceId.startsWith('gutenberg:');
      const title = normalizeMainTitle(rawTitle);
      const book: ApiEbook = {
        id,
        title,
        author: String(r?.author || ''),
        language: String(r?.lang_code || lang),
        category: String(r?.category || categoryRaw),
        coverUrl: r?.cover_url ?? null,
        downloadUrl: r?.download_url ?? null,
      };
      if (isGutenberg) gutenbergTitleCandidates.push(book);
      return book;
    };

    // Total from precomputed counts (fast).
    const countResp = await supabase
      .from(EBOOK_CATALOG_CATEGORY_COUNTS_TABLE)
      .select('category,count')
      .eq('lang_code', lang)
      .eq('category', categoryRaw)
      .maybeSingle();
    if (countResp.error) throw countResp.error;

    const total = typeof (countResp.data as any)?.count === 'number' ? Math.max(0, Math.floor((countResp.data as any).count)) : 0;
    const category = typeof (countResp.data as any)?.category === 'string' ? String((countResp.data as any).category).trim() : categoryRaw;

    const itemsResp = await supabase
      .from(EBOOK_CATALOG_ITEMS_TABLE)
      .select('id,title,author,lang_code,category,cover_url,download_url,source,source_id')
      .eq('lang_code', lang)
      .eq('category', category)
      .neq('download_url', '')
      .order('source_popularity', { ascending: false, nullsFirst: false })
      .order('updated_at', { ascending: false })
      .range(cursor, cursor + limit - 1);
    if (itemsResp.error) throw itemsResp.error;

    const slice = (Array.isArray(itemsResp.data) ? itemsResp.data : []).map(toApiFromRow);
    if (!slice.length) {
      if (allowEmpty) {
        const payload: ApiEbooksCategoryResponse = {
          lang,
          category,
          generatedAt: new Date().toISOString(),
          total: 0,
          cursor,
          limit,
          nextCursor: null,
          items: [],
        };
        res.setHeader('Cache-Control', 'private, max-age=300');
        res.json(payload);
      } else {
      res.status(404).json({ message: 'Category not found' });
      }
      return;
    }

    await enrichGutenbergTitlesWithOpenLibrary(lang, gutenbergTitleCandidates);

    const nextCursor = total > 0 && cursor + slice.length < total ? cursor + slice.length : null;

    const payload: ApiEbooksCategoryResponse = {
      lang,
      category,
      generatedAt: new Date().toISOString(),
      total: total > 0 ? total : cursor + slice.length,
      cursor,
      limit,
      nextCursor,
      items: slice,
    };

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.json(payload);
  } catch (error) {
    console.error('getEbooksCategory failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

type ApiEbooksSearchResponse = {
  lang: string;
  query: string;
  generatedAt: string;
  total: number;
  cursor: number;
  limit: number;
  nextCursor: number | null;
  items: ApiEbook[];
};

// Search across the DB-backed catalog for a learning language.
// NOTE: This is intentionally scoped to the catalog (not OPDS browsing) so it scales to large libraries.
export const getEbooksSearch = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const qRaw =
      typeof req.query.q === 'string'
        ? req.query.q.trim()
        : typeof (req.query as any)?.query === 'string'
          ? String((req.query as any).query).trim()
          : '';
    if (!qRaw || qRaw.length < 2) {
      res.status(400).json({ message: 'Missing query' });
      return;
    }

    const cursor = clampInt((req.query.cursor ?? req.query.offset) as unknown, 0, 9999999, 0);
    const limit = clampInt((req.query.pageSize ?? req.query.limit) as unknown, 10, 100, 40);

    const isMissingCatalogError = (e: any) => {
      const msg = String(e?.message || '');
      return msg.includes('ebook_catalog_') || msg.includes('relation') || msg.includes('does not exist');
    };

    // Keep this safe for PostgREST filter strings (avoid commas/parentheses breaking `.or(...)`).
    const normalizeSearchKey = (value: string) => {
      const base = normalizeForCatalogNorm(value);
      return base.replace(/[^\p{L}\p{N} ]+/gu, ' ').replace(/\s+/g, ' ').trim();
    };

    const want = normalizeSearchKey(qRaw);
    if (!want) {
      const payload: ApiEbooksSearchResponse = {
        lang,
        query: qRaw,
        generatedAt: new Date().toISOString(),
        total: 0,
        cursor,
        limit,
        nextCursor: null,
        items: [],
      };
      res.setHeader('Cache-Control', 'private, max-age=60');
      res.json(payload);
      return;
    }

    const pattern = `%${want}%`;
    const gutenbergTitleCandidates: ApiEbook[] = [];

    const toApiFromRow = (r: any): ApiEbook => {
      const id = String(r?.id || '');
      const rawTitle = String(r?.title || '');
      const source = String(r?.source || '');
      const sourceId = String(r?.source_id || '');
      const isGutenberg = source === 'gutendex' || sourceId.startsWith('gutenberg:');
      const title = normalizeMainTitle(rawTitle);
      const book: ApiEbook = {
        id,
        title,
        author: String(r?.author || ''),
        language: String(r?.lang_code || lang),
        category: String(r?.category || 'Fiction'),
        coverUrl: r?.cover_url ?? null,
        downloadUrl: r?.download_url ?? null,
      };
      if (isGutenberg) gutenbergTitleCandidates.push(book);
      return book;
    };

    try {
      const resp = await supabase
        .from(EBOOK_CATALOG_ITEMS_TABLE)
        .select('id,title,author,lang_code,category,cover_url,download_url,source,source_id', { count: 'exact' })
        .eq('lang_code', lang)
        .neq('download_url', '')
        .or(`title_norm.ilike.${pattern},author_norm.ilike.${pattern}`)
        .order('source_popularity', { ascending: false, nullsFirst: false })
        .order('updated_at', { ascending: false })
        .range(cursor, cursor + limit - 1);
      if (resp.error) throw resp.error;

      const slice = (Array.isArray(resp.data) ? resp.data : []).map(toApiFromRow);
      await enrichGutenbergTitlesWithOpenLibrary(lang, gutenbergTitleCandidates);

      const total = typeof resp.count === 'number' ? Math.max(0, Math.floor(resp.count)) : 0;
      const nextCursor =
        total > 0 && cursor + slice.length < total ? cursor + slice.length : slice.length >= limit ? cursor + slice.length : null;

      const payload: ApiEbooksSearchResponse = {
        lang,
        query: qRaw,
        generatedAt: new Date().toISOString(),
        total: total > 0 ? total : cursor + slice.length,
        cursor,
        limit,
        nextCursor,
        items: slice,
      };

      res.setHeader('Cache-Control', 'private, max-age=60');
      res.json(payload);
      return;
    } catch (e) {
      if (isMissingCatalogError(e)) {
        res.status(500).json({ message: 'Ebook catalog is not initialized. Run sql/ebooks_catalog.sql in Supabase first.' });
        return;
      }
      throw e;
    }
  } catch (error) {
    console.error('getEbooksSearch failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

type ApiEbooksAuthorResponse = {
  lang: string;
  author: string;
  generatedAt: string;
  total: number;
  cursor: number;
  limit: number;
  nextCursor: number | null;
  items: ApiEbook[];
};

// Manual trigger (useful for dev / admin tooling).
export const postEbookCatalogSync = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const bodyLang = normalizeLang((req.body as any)?.lang);
    const queryLang = normalizeLang(req.query.lang);
    const lang = bodyLang ?? queryLang ?? userLang ?? 'en';

    const result = await syncEbookCatalogLanguage(lang);
    res.json({ lang, ...result });
  } catch (error) {
    console.error('postEbookCatalogSync failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

type CatalogImportSource = 'manybooks';
type CatalogImportItem = {
  title: string;
  author: string;
  category?: string | null;
  coverUrl?: string | null;
  downloadUrl: string;
  sourceId?: string | null;
  sourcePopularity?: number | null;
};

const isAllowedManyBooksHost = (hostname: string) => {
  const host = String(hostname || '').toLowerCase();
  return host === 'manybooks.net' || host.endsWith('.manybooks.net');
};

const normalizeCatalogCategory = (raw: unknown) => {
  const s = typeof raw === 'string' ? raw.trim() : '';
  // Keep category human-friendly; fall back to Fiction.
  const cleaned = s.replace(/\s+/g, ' ').slice(0, 40).trim();
  return cleaned || 'Fiction';
};

export const postEbookCatalogImport = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const body = (req.body || {}) as any;
    const source = String(body?.source || '').trim().toLowerCase() as CatalogImportSource;
    if (source !== 'manybooks') {
      res.status(400).json({ message: 'Unsupported source' });
      return;
    }

    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const bodyLang = normalizeLang(body?.lang);
    const queryLang = normalizeLang(req.query.lang);
    const lang = bodyLang ?? queryLang ?? userLang ?? 'en';

    const itemsRaw = Array.isArray(body?.items) ? body.items : [];
    if (!itemsRaw.length) {
      res.status(400).json({ message: 'items is required' });
      return;
    }

    // Bound payload size to keep the API safe (client can call again later).
    const maxItems = clampInt(body?.maxItems, 20, 400, 180);
    const slice = itemsRaw.slice(0, maxItems);

    const nowIso = new Date().toISOString();

    // Validate + normalize items, dedupe by catalog id.
    const deduped = new Map<string, EbookCatalogItemRow>();
    const rejected: Array<{ reason: string }> = [];

    for (const raw of slice) {
      const title = typeof raw?.title === 'string' ? raw.title.trim() : '';
      const author = typeof raw?.author === 'string' ? raw.author.trim() : '';
      const downloadUrl = typeof raw?.downloadUrl === 'string' ? raw.downloadUrl.trim() : '';
      const coverUrl = typeof raw?.coverUrl === 'string' ? raw.coverUrl.trim() : '';
      const sourceId = typeof raw?.sourceId === 'string' ? raw.sourceId.trim() : '';
      const category = normalizeCatalogCategory(raw?.category);

      if (!title || title.length < 2) {
        rejected.push({ reason: 'missing_title' });
        continue;
      }
      if (!author || author.toLowerCase() === 'unknown') {
        rejected.push({ reason: 'missing_author' });
        continue;
      }
      if (!downloadUrl) {
        rejected.push({ reason: 'missing_download_url' });
        continue;
      }

      let dlParsed: URL;
      try {
        dlParsed = new URL(downloadUrl);
      } catch {
        rejected.push({ reason: 'invalid_download_url' });
        continue;
      }
      if (dlParsed.protocol !== 'http:' && dlParsed.protocol !== 'https:') {
        rejected.push({ reason: 'invalid_download_url_protocol' });
        continue;
      }
      if (!isAllowedManyBooksHost(dlParsed.hostname)) {
        rejected.push({ reason: 'download_host_not_allowed' });
        continue;
      }

      let coverOk: string | null = null;
      if (coverUrl) {
        try {
          const cu = new URL(coverUrl);
          if (cu.protocol === 'http:' || cu.protocol === 'https:') coverOk = coverUrl;
        } catch {
          // ignore invalid cover urls
        }
      }

      const id = toCatalogId(lang, title, author);
      const row: EbookCatalogItemRow = {
        id,
        lang_code: lang,
        title,
        author,
        category,
        cover_url: coverOk,
        download_url: downloadUrl,
        source: source,
        source_id: sourceId || null,
        source_popularity: typeof raw?.sourcePopularity === 'number' ? Math.max(0, Math.floor(raw.sourcePopularity)) : null,
        author_norm: normalizeForCatalogNorm(author),
        title_norm: normalizeForCatalogNorm(title),
        updated_at: nowIso,
        last_seen_at: nowIso,
      };

      const prev = deduped.get(id);
      if (!prev) {
        deduped.set(id, row);
      } else {
        // Prefer a cover if we didn't have one yet. Keep the first downloadUrl (we'll never override existing DB download_url anyway).
        if (!prev.cover_url && row.cover_url) prev.cover_url = row.cover_url;
        if (!prev.source_id && row.source_id) prev.source_id = row.source_id;
        if (!prev.source_popularity && row.source_popularity) prev.source_popularity = row.source_popularity;
      }
    }

    const incoming = Array.from(deduped.values());
    if (!incoming.length) {
      console.log(`[catalog-import] source=${source} lang=${lang} received=${slice.length} accepted=0 upserted=0 inserted=0 updated=0 rejected=${rejected.length}`);
      res.json({ lang, source, received: slice.length, accepted: 0, upserted: 0, inserted: 0, updated: 0, rejected: rejected.length });
      return;
    }

    const fetchExistingByIds = async (ids: string[]) => {
      if (!ids.length) return new Map<string, any>();
      const resp = await supabase
        .from(EBOOK_CATALOG_ITEMS_TABLE)
        .select('id,lang_code,title,author,category,cover_url,download_url,source,source_id,source_popularity,author_norm,title_norm')
        .in('id', ids);
      if (resp.error) throw resp.error;
      const rows = Array.isArray(resp.data) ? resp.data : [];
      return new Map(rows.map((r: any) => [String(r?.id || ''), r]));
    };

    // Only upsert:
    // - new ids (insert)
    // - existing ids when we can upgrade cover_url (don't override download_url/source)
    const toUpsert: EbookCatalogItemRow[] = [];
    let inserted = 0;
    let updated = 0;

    const batchSize = 200;
    for (let i = 0; i < incoming.length; i += batchSize) {
      const batch = incoming.slice(i, i + batchSize);
      const ids = batch.map((b) => b.id);
      const existingById = await fetchExistingByIds(ids);

      for (const row of batch) {
        const existing = existingById.get(row.id);
        if (!existing) {
          inserted += 1;
          toUpsert.push(row);
          continue;
        }

        const existingCover = String(existing?.cover_url || '').trim();
        const incomingCover = String(row.cover_url || '').trim();

        const canUpgradeCover =
          !!incomingCover &&
          (!existingCover || isProbablyGenericGutenbergCover(existingCover));

        const existingDl = String(existing?.download_url || '').trim();
        const canBackfillDownloadUrl = !existingDl && !!row.download_url;

        if (!canUpgradeCover && !canBackfillDownloadUrl) continue;

        updated += 1;
        const merged: EbookCatalogItemRow = {
          id: String(existing.id),
          lang_code: String(existing.lang_code || lang),
          title: String(existing.title || row.title),
          author: String(existing.author || row.author),
          category: String(existing.category || row.category || 'Fiction'),
          cover_url: canUpgradeCover ? incomingCover : (existingCover || null),
          download_url: existingDl || row.download_url,
          // Preserve the existing selected source unless we had no download_url before.
          source: canBackfillDownloadUrl ? source : String(existing.source || source),
          source_id: canBackfillDownloadUrl ? (row.source_id || null) : (existing.source_id ?? null),
          source_popularity: typeof existing.source_popularity === 'number' ? existing.source_popularity : (row.source_popularity ?? null),
          author_norm: String(existing.author_norm || normalizeForCatalogNorm(String(existing.author || row.author))),
          title_norm: String(existing.title_norm || normalizeForCatalogNorm(String(existing.title || row.title))),
          updated_at: nowIso,
          last_seen_at: nowIso,
        };
        toUpsert.push(merged);
      }
    }

    if (toUpsert.length) {
      await upsertCatalogItems(toUpsert);

      // Clear in-memory /ebooks overview cache for this language so clients can see changes immediately.
      for (const key of Array.from(cache.keys())) {
        if (key.startsWith(`catalog:${lang}:`)) cache.delete(key);
      }

      // Keep category counts fresh so newly ingested categories show up immediately in /ebooks.
      // We do a small incremental update for touched categories on every import, and occasionally run a full rebuild.
      const touchedCategories = uniq(
        toUpsert
          .map((r) => String((r as any)?.category || '').trim())
          .filter((c) => c && c.length > 0)
      );
      if (touchedCategories.length) {
        try {
          await mapWithConcurrency(touchedCategories.slice(0, 60), 6, async (category) => {
            const countResp = await supabase
              .from(EBOOK_CATALOG_ITEMS_TABLE)
              .select('id', { count: 'exact', head: true })
              .eq('lang_code', lang)
              .eq('category', category);
            if (countResp.error) throw countResp.error;
            const count = typeof countResp.count === 'number' ? Math.max(0, Math.floor(countResp.count)) : 0;

            const upsertResp = await supabase.from(EBOOK_CATALOG_CATEGORY_COUNTS_TABLE).upsert(
              [
                {
                  lang_code: lang,
                  category,
                  count,
                  updated_at: nowIso,
                } as any,
              ],
              { onConflict: 'lang_code,category', defaultToNull: false }
            );
            if (upsertResp.error) throw upsertResp.error;
            return true;
          });
        } catch (e) {
          console.warn('[catalog-import] incremental category counts failed (non-fatal)', e);
        }
      }

      // Best-effort full rebuild (throttled). This keeps author counts + any edge cases consistent.
      try {
        const now = Date.now();
        const last = countsRebuildLastAt.get(lang) || 0;
        if (now - last >= COUNTS_REBUILD_THROTTLE_MS) {
          // Throttle even if the RPC is missing/misconfigured to avoid log spam.
          countsRebuildLastAt.set(lang, now);

          const tryRpc = async (args: Record<string, any>) => {
            const rpc = await supabase.rpc('ebook_catalog_rebuild_counts', args as any);
          if (rpc.error) throw rpc.error;
          };

          try {
            await tryRpc({ p_lang: lang });
          } catch (e1) {
            try {
              await tryRpc({ lang });
            } catch (e2) {
              try {
                await tryRpc({ lang_code: lang });
              } catch (e3) {
                await tryRpc({ p_lang_code: lang });
              }
            }
          }
        }
      } catch (e) {
        console.warn('[catalog-import] rebuild_counts failed (non-fatal)', e);
      }
    }

    console.log(
      `[catalog-import] source=${source} lang=${lang} received=${slice.length} accepted=${incoming.length} upserted=${toUpsert.length} inserted=${inserted} updated=${updated} rejected=${rejected.length}`
    );
    res.json({
      lang,
      source,
      received: slice.length,
      accepted: incoming.length,
      upserted: toUpsert.length,
      inserted,
      updated,
      rejected: rejected.length,
    });
  } catch (error) {
    console.error('postEbookCatalogImport failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

export const getEbookCatalogStatus = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const state = await supabase
      .from(EBOOK_CATALOG_SYNC_STATE_TABLE)
      .select('lang_code,status,last_started_at,last_completed_at,last_error,last_items_upserted,updated_at')
      .eq('lang_code', lang)
      .maybeSingle();
    if (state.error) throw state.error;

    const totalResp = await supabase
      .from(EBOOK_CATALOG_ITEMS_TABLE)
      .select('id', { count: 'exact', head: true })
      .eq('lang_code', lang);
    if (totalResp.error) throw totalResp.error;

    res.json({
      lang,
      status: (state.data as any)?.status ?? 'unknown',
      lastStartedAt: (state.data as any)?.last_started_at ?? null,
      lastCompletedAt: (state.data as any)?.last_completed_at ?? null,
      lastError: (state.data as any)?.last_error ?? null,
      lastUpserted: (state.data as any)?.last_items_upserted ?? null,
      total: totalResp.count ?? 0,
    });
  } catch (error) {
    console.error('getEbookCatalogStatus failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};

// Paginated access to all books for a single author (scalable browsing).
export const getEbooksAuthor = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const userLang = normalizeLang((req.user?.settings ?? {})['translationLanguage']);
    const queryLang = normalizeLang(req.query.lang);
    const lang = queryLang ?? userLang ?? 'en';

    const authorRaw = typeof req.query.author === 'string' ? req.query.author.trim() : '';
    if (!authorRaw) {
      res.status(400).json({ message: 'Missing author' });
      return;
    }

    const cursor = clampInt((req.query.cursor ?? req.query.offset) as unknown, 0, 9999999, 0);
    const limit = clampInt((req.query.pageSize ?? req.query.limit) as unknown, 10, 100, 40);

    const normalizeAuthorKey = (value: string) =>
      normalizeApostrophes(String(value || ''))
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase();

    const want = normalizeAuthorKey(authorRaw);
    const wantNorm = normalizeForCatalogNorm(want);

    const gutenbergTitleCandidates: ApiEbook[] = [];

    const toApiFromRow = (r: any): ApiEbook => {
      const id = String(r?.id || '');
      const rawTitle = String(r?.title || '');
      const source = String(r?.source || '');
      const sourceId = String(r?.source_id || '');
      const isGutenberg = source === 'gutendex' || sourceId.startsWith('gutenberg:');
      const title = normalizeMainTitle(rawTitle);
      const book: ApiEbook = {
        id,
        title,
        author: String(r?.author || ''),
        language: String(r?.lang_code || lang),
        category: String(r?.category || 'Fiction'),
        coverUrl: r?.cover_url ?? null,
        downloadUrl: r?.download_url ?? null,
      };
      if (isGutenberg) gutenbergTitleCandidates.push(book);
      return book;
    };

    // Total + canonical author name from precomputed counts.
    const countResp = await supabase
      .from(EBOOK_CATALOG_AUTHOR_COUNTS_TABLE)
      .select('author,count')
      .eq('lang_code', lang)
      .eq('author_norm', wantNorm)
      .maybeSingle();
    if (countResp.error) throw countResp.error;

    const total = typeof (countResp.data as any)?.count === 'number' ? Math.max(0, Math.floor((countResp.data as any).count)) : 0;
    const resolvedName = typeof (countResp.data as any)?.author === 'string' ? String((countResp.data as any).author).trim() : authorRaw;

    const itemsResp = await supabase
      .from(EBOOK_CATALOG_ITEMS_TABLE)
      .select('id,title,author,lang_code,category,cover_url,download_url,author_norm,source,source_id')
      .eq('lang_code', lang)
      .eq('author_norm', wantNorm)
      .neq('download_url', '')
      .order('source_popularity', { ascending: false, nullsFirst: false })
      .order('updated_at', { ascending: false })
      .range(cursor, cursor + limit - 1);
    if (itemsResp.error) throw itemsResp.error;

    const slice = (Array.isArray(itemsResp.data) ? itemsResp.data : []).map(toApiFromRow);
    if (!slice.length) {
      res.status(404).json({ message: 'Author not found' });
      return;
    }

    await enrichGutenbergTitlesWithOpenLibrary(lang, gutenbergTitleCandidates);

    const nextCursor = total > 0 && cursor + slice.length < total ? cursor + slice.length : null;

    const payload: ApiEbooksAuthorResponse = {
      lang,
      author: resolvedName,
      generatedAt: new Date().toISOString(),
      total: total > 0 ? total : cursor + slice.length,
      cursor,
      limit,
      nextCursor,
      items: slice,
    };

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.json(payload);
  } catch (error) {
    console.error('getEbooksAuthor failed', error);
    res.status(500).json({ message: 'Server error' });
  }
};
