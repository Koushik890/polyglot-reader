import type { Response } from 'express';
import type { AuthRequest } from '../types/auth';
import { createHash } from 'crypto';

type EbookTextStats = {
  totalWords: number;
  uniqueWords: number;
  readingDifficulty: number; // 0-5 (0 = unknown)
  proficiencyLevel: string; // e.g. A1..C2 or "—" when unknown
};

type EbookTextStatsResponse = EbookTextStats & {
  computedAt: string;
  cached: boolean;
};

const CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 30; // 30 days
const CACHE_MAX_ENTRIES = 500;

const statsCache = new Map<string, { value: Omit<EbookTextStatsResponse, 'cached'>; expiresAt: number }>();
const inflight = new Map<string, Promise<Omit<EbookTextStatsResponse, 'cached'>>>();

const ALLOWED_DOWNLOAD_HOSTS = [
  'gutenberg.org',
  'standardebooks.org',
  'ws-export.wmcloud.org',
  'wolnelektury.pl',
  'manybooks.net',
];

const isAllowedHost = (hostname: string) => {
  const host = (hostname || '').toLowerCase();
  return ALLOWED_DOWNLOAD_HOSTS.some((allowed) => host === allowed || host.endsWith(`.${allowed}`));
};

const clamp01 = (v: number) => Math.max(0, Math.min(1, v));

const decodeHtmlEntities = (value: string) =>
  value
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\d+);/g, (_m, d) => {
      const n = parseInt(String(d), 10);
      if (!Number.isFinite(n) || n < 0 || n > 0x10ffff) return '';
      try {
        return String.fromCodePoint(n);
      } catch {
        return '';
      }
    })
    .replace(/&#x([0-9a-fA-F]+);/g, (_m, hex) => {
      const n = parseInt(String(hex), 16);
      if (!Number.isFinite(n) || n < 0 || n > 0x10ffff) return '';
      try {
        return String.fromCodePoint(n);
      } catch {
        return '';
      }
    });

const htmlToTextBasic = (html: string) => {
  let out = String(html || '');
  out = out.replace(/<script[\s\S]*?<\/script>/gi, ' ');
  out = out.replace(/<style[\s\S]*?<\/style>/gi, ' ');
  out = out.replace(/<br\s*\/?>/gi, '\n');
  out = out.replace(/<\/(p|div|li|tr|h\d)>/gi, '\n');
  out = out.replace(/<[^>]+>/g, ' ');
  out = decodeHtmlEntities(out);
  // Keep punctuation for sentence detection, but normalize whitespace.
  out = out.replace(/[ \t\r\f\v]+/g, ' ');
  out = out.replace(/\n{3,}/g, '\n\n');
  return out.trim();
};

type TextAcc = {
  totalWords: number;
  unique: Set<string>;
  wordLenTotal: number;
  longWordCount: number;
  sentenceEndCount: number;
};

const WORD_RE = /\p{L}+(?:[-'’]\p{L}+)*/gu;
const SENTENCE_END_RE = /[.!?…]+/g;

const processText = (text: string, acc: TextAcc) => {
  if (!text) return;

  SENTENCE_END_RE.lastIndex = 0;
  while (SENTENCE_END_RE.exec(text)) acc.sentenceEndCount += 1;

  WORD_RE.lastIndex = 0;
  let m: RegExpExecArray | null = null;
  while ((m = WORD_RE.exec(text))) {
    const raw = m[0];
    if (!raw) continue;
    acc.totalWords += 1;
    acc.wordLenTotal += raw.length;
    if (raw.length >= 7) acc.longWordCount += 1;
    acc.unique.add(raw.toLowerCase());
  }
};

const guessFormat = (buffer: Buffer) => {
  if (!buffer || buffer.length < 4) return 'unknown';
  const header4 = buffer.subarray(0, 4).toString('utf8');
  if (header4 === '%PDF') return 'pdf';
  if (buffer[0] === 0x50 && buffer[1] === 0x4b) return 'epub'; // ZIP (most epubs)
  return 'txt';
};

async function downloadBinary(url: string, opts: { timeoutMs: number; maxBytes: number }): Promise<Buffer> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1_000, opts.timeoutMs));
  try {
    const resp = await fetch(url, {
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        accept: '*/*',
        'user-agent': 'PolyglotReader/1.0 (+text-stats)',
      },
    });
    if (!resp.ok) throw new Error(`Download failed (${resp.status})`);

    const contentLength = resp.headers.get('content-length');
    if (contentLength) {
      const n = parseInt(contentLength, 10);
      if (Number.isFinite(n) && n > opts.maxBytes) {
        throw new Error(`File too large (${n} bytes)`);
      }
    }

    const reader = resp.body?.getReader?.();
    if (!reader) {
      const buf = Buffer.from(await resp.arrayBuffer());
      if (buf.length > opts.maxBytes) throw new Error(`File too large (${buf.length} bytes)`);
      return buf;
    }

    const chunks: Uint8Array[] = [];
    let received = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      received += value.byteLength;
      if (received > opts.maxBytes) {
        controller.abort();
        throw new Error(`File too large (${received} bytes)`);
      }
      chunks.push(value);
    }
    return Buffer.concat(chunks.map((c) => Buffer.from(c)));
  } finally {
    clearTimeout(timer);
  }
}

async function computeStats(downloadUrl: string): Promise<EbookTextStats> {
  const buffer = await downloadBinary(downloadUrl, { timeoutMs: 25_000, maxBytes: 30 * 1024 * 1024 });
  const format = guessFormat(buffer);

  const acc: TextAcc = {
    totalWords: 0,
    unique: new Set<string>(),
    wordLenTotal: 0,
    longWordCount: 0,
    sentenceEndCount: 0,
  };

  if (format === 'pdf') {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const pdfParse = require('pdf-parse');
    const parsed = await pdfParse(buffer);
    processText(String(parsed?.text || ''), acc);
  } else if (format === 'epub') {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const AdmZip = require('adm-zip');
    const zip = new AdmZip(buffer);
    const entries = zip.getEntries?.() || [];
    for (const entry of entries) {
      if (!entry || entry.isDirectory) continue;
      const name = String(entry.entryName || '').toLowerCase();
      if (name.endsWith('.xhtml') || name.endsWith('.html') || name.endsWith('.htm')) {
        const html = entry.getData().toString('utf8');
        const text = htmlToTextBasic(html);
        processText(text, acc);
      } else if (name.endsWith('.txt')) {
        const text = entry.getData().toString('utf8');
        processText(text, acc);
      }
    }
  } else {
    processText(buffer.toString('utf8'), acc);
  }

  const totalWords = Math.max(0, Math.floor(acc.totalWords));
  const uniqueWords = Math.max(0, Math.floor(acc.unique.size));
  if (!totalWords) {
    return { totalWords, uniqueWords, readingDifficulty: 0, proficiencyLevel: '—' };
  }

  const avgWordLen = acc.wordLenTotal / totalWords;
  const longWordRatio = acc.longWordCount / totalWords;
  const sentenceCount = Math.max(1, acc.sentenceEndCount || 0);
  const avgSentenceLen = totalWords / sentenceCount;
  const uniqueRatio = uniqueWords / totalWords;

  const sLenN = clamp01((avgSentenceLen - 8) / (24 - 8));
  const wordLenN = clamp01((avgWordLen - 4) / (7 - 4));
  const longN = clamp01((longWordRatio - 0.05) / (0.25 - 0.05));
  const uniqN = clamp01((uniqueRatio - 0.3) / (0.6 - 0.3));

  const difficultyIndex = clamp01(0.4 * sLenN + 0.3 * wordLenN + 0.2 * longN + 0.1 * uniqN);
  const readingDifficulty = Math.max(1, Math.min(5, 1 + Math.round(difficultyIndex * 4)));

  const proficiencyLevel =
    difficultyIndex < 0.18
      ? 'A1'
      : difficultyIndex < 0.32
        ? 'A2'
        : difficultyIndex < 0.5
          ? 'B1'
          : difficultyIndex < 0.68
            ? 'B2'
            : difficultyIndex < 0.85
              ? 'C1'
              : 'C2';

  return {
    totalWords,
    uniqueWords,
    readingDifficulty,
    proficiencyLevel,
  };
}

export const postEbookTextStats = async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const downloadUrlRaw = typeof req.body?.downloadUrl === 'string' ? req.body.downloadUrl : '';
    const downloadUrl = String(downloadUrlRaw || '').trim();
    if (!downloadUrl) {
      res.status(400).json({ message: 'downloadUrl is required' });
      return;
    }

    let parsed: URL;
    try {
      parsed = new URL(downloadUrl);
    } catch {
      res.status(400).json({ message: 'Invalid downloadUrl' });
      return;
    }

    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      res.status(400).json({ message: 'Invalid downloadUrl protocol' });
      return;
    }
    if (parsed.username || parsed.password) {
      res.status(400).json({ message: 'Invalid downloadUrl' });
      return;
    }
    if (!isAllowedHost(parsed.hostname)) {
      res.status(400).json({ message: 'Unsupported downloadUrl host' });
      return;
    }

    const key = createHash('sha256').update(downloadUrl).digest('hex');
    const now = Date.now();
    const cached = statsCache.get(key);
    if (cached && cached.expiresAt > now) {
      res.json({ ...cached.value, cached: true } satisfies EbookTextStatsResponse);
      return;
    }

    let p = inflight.get(key);
    if (!p) {
      p = (async () => {
        const stats = await computeStats(downloadUrl);
        return {
          ...stats,
          computedAt: new Date().toISOString(),
        };
      })().finally(() => {
        inflight.delete(key);
      });
      inflight.set(key, p);
    }

    const value = await p;
    statsCache.set(key, { value, expiresAt: now + CACHE_TTL_MS });
    if (statsCache.size > CACHE_MAX_ENTRIES) {
      const first = statsCache.keys().next().value as string | undefined;
      if (first) statsCache.delete(first);
    }

    res.json({ ...value, cached: false } satisfies EbookTextStatsResponse);
  } catch (e) {
    console.error('[ebooks/stats] failed', e);
    res.status(500).json({ message: 'Server error' });
  }
};

