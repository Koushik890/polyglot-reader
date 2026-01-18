import { Response } from 'express';
import { PDFDocument } from 'pdf-lib';
import { randomUUID } from 'crypto';
import { once } from 'events';
import fs from 'fs';
import path from 'path';
import { supabase } from '../config/supabase';
import type { AuthRequest } from '../types/auth';

type DocumentFormat = 'PDF' | 'EPUB' | 'MOBI' | 'AZW' | 'AZW3' | 'KF8' | 'FB2' | 'FBZ' | 'CBZ' | 'TXT' | 'UNKNOWN';

type ImportJobStatus = 'queued' | 'downloading' | 'processing' | 'done' | 'error';
type ImportJob = {
    id: string;
    ownerId: string;
    status: ImportJobStatus;
    progress: number; // 0..1
    message?: string;
    error?: string;
    document?: any;
    createdAt: string;
    updatedAt: string;
};

type DocumentTextStats = {
    totalWords: number;
    uniqueWords: number;
    readingDifficulty: number; // 0-5 (0 = unknown)
    proficiencyLevel: string; // e.g. A1..C2 or "—"
};

type DocumentTextStatsResponse = DocumentTextStats & {
    computedAt: string;
    cached: boolean;
};

// In-memory job store (good enough for dev / single instance).
// In production, use a durable queue (BullMQ/Redis) + persistent job state.
const importJobs = new Map<string, ImportJob>();
const IMPORT_JOB_TTL_MS = 1000 * 60 * 60; // 1 hour
const IMPORT_MAX_BYTES = 200 * 1024 * 1024; // 200MB safety cap

// In-memory text stats cache (per document).
const DOC_STATS_CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 30; // 30 days
const docStatsCache = new Map<string, { value: Omit<DocumentTextStatsResponse, 'cached'>; expiresAt: number }>();
const docStatsInflight = new Map<string, Promise<Omit<DocumentTextStatsResponse, 'cached'>>>();

const clamp01 = (v: number) => Math.max(0, Math.min(1, v));

type TextAcc = {
    totalWords: number;
    unique: Set<string>;
    wordLenTotal: number;
    longWordCount: number;
    sentenceEndCount: number;
};

const WORD_RE = /\p{L}+(?:[-'’]\p{L}+)*/gu;
const SENTENCE_END_RE = /[.!?…]+/g;

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
    out = out.replace(/[ \t\r\f\v]+/g, ' ');
    out = out.replace(/\n{3,}/g, '\n\n');
    return out.trim();
};

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

const guessFormatFromBuffer = (buffer: Buffer) => {
    if (!buffer || buffer.length < 4) return 'unknown';
    const header4 = buffer.subarray(0, 4).toString('utf8');
    if (header4 === '%PDF') return 'pdf';
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return 'epub'; // ZIP (most epubs)
    return 'txt';
};

async function computeDocumentTextStatsFromFile(filePath: string): Promise<DocumentTextStats> {
    // Safety cap: avoid blocking the server on huge files.
    const stat = fs.statSync(filePath);
    const MAX_BYTES = 30 * 1024 * 1024;
    if (stat.size > MAX_BYTES) {
        return { totalWords: 0, uniqueWords: 0, readingDifficulty: 0, proficiencyLevel: '—' };
    }
    const buffer = fs.readFileSync(filePath);
    const format = guessFormatFromBuffer(buffer);

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

    return { totalWords, uniqueWords, readingDifficulty, proficiencyLevel };
}

const ALLOWED_IMPORT_HOSTS = [
    'gutenberg.org',
    'standardebooks.org',
    'ws-export.wmcloud.org',
    'wolnelektury.pl',
    'manybooks.net',
];

const isAllowedImportHost = (hostname: string) => {
    const host = (hostname || '').toLowerCase();
    return ALLOWED_IMPORT_HOSTS.some((allowed) => host === allowed || host.endsWith(`.${allowed}`));
};

const BROWSER_UA =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const headersForImportFetch = (downloadUrl: string): Record<string, string> => {
    let host = '';
    try {
        host = new URL(downloadUrl).hostname.toLowerCase();
    } catch {}

    // ManyBooks is often Cloudflare-protected; a browser UA + referer is more reliable.
    if (host === 'manybooks.net' || host.endsWith('.manybooks.net')) {
        return {
            accept: '*/*',
            'user-agent': BROWSER_UA,
            referer: 'https://manybooks.net/',
        };
    }

    return {
        accept: '*/*',
        'user-agent': 'PolyglotReader/1.0 (+server-import)',
    };
};

const pruneImportJobs = () => {
    const now = Date.now();
    for (const [id, job] of importJobs.entries()) {
        const updatedAt = Date.parse(job.updatedAt);
        if (!Number.isFinite(updatedAt)) continue;
        if (now - updatedAt > IMPORT_JOB_TTL_MS) importJobs.delete(id);
    }
};

const updateImportJob = (jobId: string, patch: Partial<ImportJob>) => {
    const existing = importJobs.get(jobId);
    if (!existing) return;
    const next: ImportJob = {
        ...existing,
        ...patch,
        updatedAt: new Date().toISOString(),
    };
    // Clamp progress
    if (typeof next.progress === 'number') {
        next.progress = Math.max(0, Math.min(1, next.progress));
    }
    importJobs.set(jobId, next);
};

const sleepMs = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, Math.floor(ms || 0))));

const sanitizeFileSegment = (raw: string) =>
    (raw || 'book')
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, 80)
        .replace(/[<>:"/\\|?*\u0000-\u001F]+/g, '_')
        .replace(/\s/g, '_');

const sniffFileHead = (filePath: string, maxBytes = 96) => {
    try {
        const fd = fs.openSync(filePath, 'r');
        try {
            const buf = Buffer.allocUnsafe(Math.max(1, maxBytes));
            const read = fs.readSync(fd, buf, 0, buf.length, 0);
            return buf.subarray(0, Math.max(0, read));
        } finally {
            fs.closeSync(fd);
        }
    } catch {
        return Buffer.from([]);
    }
};

const isHtmlLike = (head: Buffer) => {
    const s = head.toString('utf8').trimStart().toLowerCase();
    return s.startsWith('<!doctype') || s.startsWith('<html') || s.startsWith('<head') || s.startsWith('<script') || s.startsWith('<meta');
};

const extFromUrl = (downloadUrl: string) => {
    try {
        const u = new URL(downloadUrl);
        const lower = u.pathname.toLowerCase();
        if (lower.endsWith('.fb2.zip')) return '.fb2.zip';
        const ext = path.extname(lower);
        const allowed = new Set(['.pdf', '.epub', '.mobi', '.azw', '.azw3', '.kf8', '.fb2', '.fbz', '.cbz', '.txt']);
        return allowed.has(ext) ? ext : '';
    } catch {
        return '';
    }
};

const extFromContentType = (contentType: string) => {
    const ct = (contentType || '').toLowerCase();
    if (ct.includes('application/pdf')) return '.pdf';
    if (ct.includes('application/epub+zip')) return '.epub';
    if (ct.includes('text/plain')) return '.txt';
    return '';
};

async function downloadToUploads(
    jobId: string,
    downloadUrl: string,
    opts: { timeoutMs: number; maxBytes: number; onProgress?: (p01: number) => void }
): Promise<{ filePath: string; size: number; originalName: string; ext: string }> {
    fs.mkdirSync('uploads', { recursive: true });
    const tmpPath = path.join('uploads', `import-${jobId}.part`);
    const startedAt = Date.now();

    const MAX_RETRIES = 3;
    let lastProgressAt = 0;

    const shouldRetryHttp = (status: number) => status === 429 || (status >= 500 && status <= 599);
    const parseRetryAfterMs = (value: string | null) => {
        if (!value) return 0;
        const seconds = parseInt(value, 10);
        if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000;
        return 0;
    };

    const cleanupPartial = () => {
        try {
            if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
        } catch {}
    };

    try {
        for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            const resumeFrom = (() => {
                try {
                    return fs.existsSync(tmpPath) ? fs.statSync(tmpPath).size : 0;
                } catch {
                    return 0;
                }
            })();

            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), Math.max(1_000, Math.floor(opts.timeoutMs || 0)));

            try {
                const headers = headersForImportFetch(downloadUrl);
                if (resumeFrom > 0) {
                    headers.Range = `bytes=${resumeFrom}-`;
                }

                const res = await fetch(downloadUrl, {
                    signal: controller.signal,
                    redirect: 'follow',
                    headers,
                });

                // Retry on rate limits / transient server errors.
                if (!res.ok && shouldRetryHttp(res.status) && attempt < MAX_RETRIES) {
                    const err: any = new Error(`Download failed (HTTP ${res.status})`);
                    err.httpStatus = res.status;
                    err.retryAfterMs = parseRetryAfterMs(res.headers.get('retry-after'));
                    throw err;
                }

                // If we attempted resume but the server didn't honor range, restart from scratch.
                const isPartial = resumeFrom > 0 && res.status === 206;
                if (resumeFrom > 0 && !isPartial) {
                    try {
                        fs.truncateSync(tmpPath, 0);
                    } catch {}
                }

                if (!res.ok) throw new Error(`Download failed (HTTP ${res.status})`);

                const contentType = res.headers.get('content-type') || '';
                if (String(contentType).toLowerCase().includes('text/html')) {
                    throw new Error('Download returned HTML instead of a book file (source may be blocking downloads).');
                }

                // Determine total size for progress & maxBytes checks.
                const contentRange = res.headers.get('content-range') || '';
                const totalFromRange = (() => {
                    const m = /\/(\d+)\s*$/i.exec(contentRange);
                    return m?.[1] ? parseInt(m[1], 10) : NaN;
                })();
                const contentLength = res.headers.get('content-length') || '';
                const len = contentLength ? parseInt(contentLength, 10) : NaN;
                const expectedTotal = Number.isFinite(totalFromRange)
                    ? totalFromRange
                    : Number.isFinite(len)
                      ? (isPartial ? resumeFrom + len : len)
                      : NaN;

                if (Number.isFinite(expectedTotal) && expectedTotal > opts.maxBytes) {
                    throw new Error(`File too large (${expectedTotal} bytes)`);
                }

                const body = res.body;
                if (!body) throw new Error('Download failed (no body)');

                const writer = fs.createWriteStream(tmpPath, { flags: isPartial ? 'a' : 'w' });
                let received = isPartial ? resumeFrom : 0;

                // Node fetch returns a WHATWG ReadableStream.
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const reader = (body as any).getReader?.();
                if (!reader) {
                    // Fallback: buffer in memory (should be rare)
                    const buf = Buffer.from(await res.arrayBuffer());
                    const nextSize = (isPartial ? resumeFrom : 0) + buf.length;
                    if (nextSize > opts.maxBytes) throw new Error(`File too large (${nextSize} bytes)`);
                    fs.appendFileSync(tmpPath, buf);
                    received = nextSize;
                } else {
                    while (true) {
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        const { done, value } = (await reader.read()) as any;
                        if (done) break;
                        if (!value) continue;
                        const chunk = Buffer.from(value);
                        received += chunk.length;
                        if (received > opts.maxBytes) {
                            controller.abort();
                            throw new Error(`File too large (${received} bytes)`);
                        }

                        if (!writer.write(chunk)) {
                            await once(writer, 'drain');
                        }

                        const now = Date.now();
                        if (opts.onProgress && now - lastProgressAt > 180) {
                            lastProgressAt = now;
                            if (Number.isFinite(expectedTotal) && expectedTotal > 0) {
                                opts.onProgress(Math.max(0, Math.min(1, received / expectedTotal)));
                            } else {
                                // Unknown total: progress ramps up slowly but never hits 100 until done.
                                const seconds = (now - startedAt) / 1000;
                                const approx = 1 - Math.exp(-seconds / 8);
                                opts.onProgress(Math.max(0, Math.min(0.9, approx)));
                            }
                        }
                    }
                }

                await new Promise<void>((resolve, reject) => {
                    writer.end(() => resolve());
                    writer.on('error', reject);
                });

                // Quick validation: ensure not HTML (some sources return an HTML error after a redirect).
                const head = sniffFileHead(tmpPath, 96);
                if (isHtmlLike(head)) {
                    throw new Error('Download returned HTML instead of a book file (source may be blocking downloads).');
                }

                // Success: break retry loop.
                break;
            } catch (err: any) {
                const httpStatus = typeof err?.httpStatus === 'number' ? err.httpStatus : 0;
                const retryable = httpStatus ? shouldRetryHttp(httpStatus) : true;
                const isAbort = err?.name === 'AbortError';

                if (attempt >= MAX_RETRIES || (!retryable && !isAbort)) {
                    throw err;
                }

                const retryAfterMs = typeof err?.retryAfterMs === 'number' ? err.retryAfterMs : 0;
                const backoff = retryAfterMs || Math.min(12_000, 800 * Math.pow(2, attempt));
                await sleepMs(backoff + Math.floor(Math.random() * 200));
                continue;
            } finally {
                clearTimeout(timer);
            }
        }

        // Validate content isn't an HTML challenge/error page and determine extension.
        const head = sniffFileHead(tmpPath, 96);
        if (isHtmlLike(head)) {
            throw new Error('Download returned HTML instead of a book file (source may be blocking downloads).');
        }

        let ext = extFromUrl(downloadUrl);
        const headText = head.toString('utf8').trimStart();
        if (!ext) {
            if (headText.startsWith('%PDF')) ext = '.pdf';
            else if (head.length >= 2 && head[0] === 0x50 && head[1] === 0x4b) ext = '.epub';
            else ext = '.txt';
        }

        if (ext === '.pdf' && !headText.startsWith('%PDF')) {
            throw new Error('Downloaded file is not a valid PDF.');
        }

        const size = (() => {
            try {
                return fs.statSync(tmpPath).size;
            } catch {
                return 0;
            }
        })();

        const base = sanitizeFileSegment(path.basename(new URL(downloadUrl).pathname) || 'book');
        const originalName = base ? `${base}${ext}` : `book${ext}`;

        const finalPath = path.join('uploads', `import-${Date.now()}-${jobId}${ext}`);
        fs.renameSync(tmpPath, finalPath);

        return { filePath: finalPath, size, originalName, ext };
    } catch (e) {
        cleanupPartial();
        throw e;
    }
}

async function createDocumentRecord(opts: {
    ownerId: string;
    filePath: string;
    originalName: string;
    title?: string;
    author?: string;
    size: number;
}): Promise<any> {
    const decodedTitle = safeDecodeUriComponent(opts.title || opts.originalName);
    const format = detectFormatFromName(opts.originalName);

    let pageCount = 0;
    if (format === 'PDF') {
        const fileBuffer = fs.readFileSync(opts.filePath);
        const pdfDoc = await PDFDocument.load(fileBuffer);
        pageCount = pdfDoc.getPageCount();
    }

    const { data: doc, error } = await supabase
        .from('documents')
        .insert({
            owner_id: opts.ownerId,
            title: decodedTitle || opts.originalName,
            author: opts.author || 'Unknown',
            file_key: opts.filePath,
            page_count: pageCount,
            last_read_page: 1,
        })
        .select('id,title,author,page_count,last_read_page,created_at,file_key')
        .single();

    if (error || !doc) {
        throw new Error('Server error during import');
    }

    return {
        id: doc.id,
        title: safeDecodeUriComponent(doc.title),
        author: doc.author,
        pageCount: doc.page_count,
        lastReadPage: doc.last_read_page,
        createdAt: doc.created_at,
        size: opts.size,
        format,
        mimeType: mimeTypeFromFormat(format),
    };
}

const URL_ENCODED_PATTERN = /%[0-9A-Fa-f]{2}/;
const safeDecodeUriComponent = (value?: unknown): string => {
    const input = typeof value === 'string' ? value : value == null ? '' : String(value);
    if (!URL_ENCODED_PATTERN.test(input)) return input;
    let out = input;
    for (let i = 0; i < 2; i++) {
        if (!URL_ENCODED_PATTERN.test(out)) break;
        try {
            const decoded = decodeURIComponent(out);
            if (decoded === out) break;
            out = decoded;
        } catch {
            break;
        }
    }
    return out;
};

const detectFormatFromName = (name?: string): DocumentFormat => {
    const lower = (name || '').toLowerCase().trim();
    if (!lower) return 'UNKNOWN';

    if (lower.endsWith('.fb2.zip') || lower.endsWith('.fbz')) return 'FBZ';

    const ext = path.extname(lower);
    switch (ext) {
        case '.pdf':
            return 'PDF';
        case '.epub':
            return 'EPUB';
        case '.mobi':
            return 'MOBI';
        case '.azw':
            return 'AZW';
        case '.azw3':
            return 'AZW3';
        case '.kf8':
            return 'KF8';
        case '.fb2':
            return 'FB2';
        case '.cbz':
            return 'CBZ';
        case '.txt':
            return 'TXT';
        default:
            return 'UNKNOWN';
    }
};

const detectFormat = (title?: string, fileKey?: string): DocumentFormat => {
    const fromTitle = detectFormatFromName(title);
    if (fromTitle !== 'UNKNOWN') return fromTitle;
    const fromFileKey = detectFormatFromName(fileKey);
    if (fromFileKey !== 'UNKNOWN') return fromFileKey;
    return 'UNKNOWN';
};

const mimeTypeFromFormat = (format: DocumentFormat): string => {
    switch (format) {
        case 'PDF':
            return 'application/pdf';
        case 'EPUB':
            return 'application/epub+zip';
        case 'CBZ':
            return 'application/vnd.comicbook+zip';
        case 'FB2':
            return 'application/x-fictionbook+xml';
        case 'FBZ':
            return 'application/x-zip-compressed-fb2';
        case 'TXT':
            return 'text/plain; charset=utf-8';
        case 'MOBI':
        case 'AZW':
        case 'AZW3':
        case 'KF8':
            return 'application/x-mobipocket-ebook';
        default:
            return 'application/octet-stream';
    }
};

export const uploadDocument = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        if (!req.file) {
            res.status(400).json({ message: 'No file uploaded' });
            return;
        }

        const ownerId = req.user?.id;
        if (!ownerId) {
            res.status(401).json({ message: 'Not authorized' });
            return;
        }
        const created = await createDocumentRecord({
            ownerId,
            filePath: req.file.path,
            originalName: req.file.originalname,
            title: req.body.title,
            author: req.body.author,
            size: req.file.size,
        });
        res.status(201).json(created);
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Server error during upload' });
    }
};

// Start a server-side import of a public-domain ebook by downloadUrl.
// Mobile polls the job status to display a smooth progress bar without client-side retries/resets.
export const postImportDocumentFromUrl = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        pruneImportJobs();

        const ownerId = req.user?.id;
        if (!ownerId) {
            res.status(401).json({ message: 'Not authorized' });
            return;
        }

        const downloadUrl = typeof req.body?.downloadUrl === 'string' ? String(req.body.downloadUrl).trim() : '';
        const title = typeof req.body?.title === 'string' ? String(req.body.title).trim() : '';
        const author = typeof req.body?.author === 'string' ? String(req.body.author).trim() : '';

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
        if (!isAllowedImportHost(parsed.hostname)) {
            res.status(400).json({ message: 'Unsupported downloadUrl host' });
            return;
        }

        const jobId = randomUUID();
        const nowIso = new Date().toISOString();
        importJobs.set(jobId, {
            id: jobId,
            ownerId,
            status: 'queued',
            progress: 0,
            message: 'Queued',
            createdAt: nowIso,
            updatedAt: nowIso,
        });

        // Fire and forget.
        (async () => {
            try {
                updateImportJob(jobId, { status: 'downloading', progress: 0.02, message: 'Downloading' });
                const downloaded = await downloadToUploads(jobId, downloadUrl, {
                    timeoutMs: 5 * 60_000,
                    maxBytes: IMPORT_MAX_BYTES,
                    onProgress: (p01) => updateImportJob(jobId, { status: 'downloading', progress: 0.02 + p01 * 0.9 }),
                });

                updateImportJob(jobId, { status: 'processing', progress: 0.96, message: 'Processing' });
                const created = await createDocumentRecord({
                    ownerId,
                    filePath: downloaded.filePath,
                    originalName: downloaded.originalName,
                    title,
                    author,
                    size: downloaded.size,
                });

                updateImportJob(jobId, { status: 'done', progress: 1, message: 'Done', document: created });
            } catch (e: any) {
                const msg = typeof e?.message === 'string' ? e.message : 'Import failed';
                updateImportJob(jobId, { status: 'error', progress: 1, message: 'Error', error: msg });
                console.warn('[documents/import] job failed', { jobId, msg });
            }
        })().catch(() => {});

        res.status(202).json({ jobId });
    } catch (e) {
        console.error('[documents/import] start failed', e);
        res.status(500).json({ message: 'Server error' });
    }
};

export const getImportDocumentJob = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        pruneImportJobs();
        const ownerId = req.user?.id;
        if (!ownerId) {
            res.status(401).json({ message: 'Not authorized' });
            return;
        }

        const jobId = typeof req.params?.jobId === 'string' ? String(req.params.jobId).trim() : '';
        const job = jobId ? importJobs.get(jobId) : null;
        if (!job || job.ownerId !== ownerId) {
            res.status(404).json({ message: 'Job not found' });
            return;
        }

        res.json({
            jobId: job.id,
            status: job.status,
            progress: job.progress,
            message: job.message,
            error: job.error,
            document: job.document,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
        });
    } catch (e) {
        console.error('[documents/import] status failed', e);
        res.status(500).json({ message: 'Server error' });
    }
};

export const getDocuments = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { data: documents, error } = await supabase
            .from('documents')
            .select('id,title,author,page_count,last_read_page,created_at,file_key')
            .eq('owner_id', req.user?.id)
            .order('updated_at', { ascending: false });

        if (error) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        res.json(
            (documents ?? []).map((doc) => ({
                id: doc.id,
                title: safeDecodeUriComponent(doc.title),
                author: doc.author,
                pageCount: doc.page_count,
                lastReadPage: doc.last_read_page,
                createdAt: doc.created_at,
                // File size isn't stored in DB; compute from local file path when available.
                size: (() => {
                    try {
                        if (doc.file_key && fs.existsSync(doc.file_key)) {
                            return fs.statSync(doc.file_key).size;
                        }
                    } catch {
                        // ignore
                    }
                    return undefined;
                })(),
                format: detectFormat(doc.title, doc.file_key),
                mimeType: mimeTypeFromFormat(detectFormat(doc.title, doc.file_key)),
            }))
        );
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const getDocumentById = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const ownerId = req.user?.id;
        if (!ownerId) {
            res.status(401).json({ message: 'Not authorized' });
            return;
        }

        const id = typeof req.params?.id === 'string' ? String(req.params.id).trim() : '';
        if (!id) {
            res.status(400).json({ message: 'Invalid id' });
            return;
        }

        const { data: doc, error } = await supabase
            .from('documents')
            .select('id,title,author,page_count,last_read_page,created_at,file_key')
            .eq('id', id)
            .eq('owner_id', ownerId)
            .maybeSingle();

        if (error || !doc) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        const format = detectFormat(doc.title, doc.file_key);
        const size = (() => {
            try {
                if (doc.file_key && fs.existsSync(doc.file_key)) return fs.statSync(doc.file_key).size;
            } catch {}
            return undefined;
        })();

        res.json({
            id: doc.id,
            title: safeDecodeUriComponent(doc.title),
            author: doc.author,
            pageCount: doc.page_count,
            lastReadPage: doc.last_read_page,
            createdAt: doc.created_at,
            size,
            format,
            mimeType: mimeTypeFromFormat(format),
        });
    } catch (e) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const getDocumentTextStats = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const ownerId = req.user?.id;
        if (!ownerId) {
            res.status(401).json({ message: 'Not authorized' });
            return;
        }

        const id = typeof req.params?.id === 'string' ? String(req.params.id).trim() : '';
        if (!id) {
            res.status(400).json({ message: 'Invalid id' });
            return;
        }

        const { data: doc, error } = await supabase
            .from('documents')
            .select('id,file_key,updated_at')
            .eq('id', id)
            .eq('owner_id', ownerId)
            .maybeSingle();

        if (error || !doc) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        const fileKey = String(doc.file_key || '').trim();
        if (!fileKey || !fs.existsSync(fileKey)) {
            res.status(404).json({ message: 'File not found' });
            return;
        }

        const cacheKey = `doc:${id}`;
        const now = Date.now();
        const cached = docStatsCache.get(cacheKey);
        if (cached && cached.expiresAt > now) {
            res.json({ ...cached.value, cached: true } satisfies DocumentTextStatsResponse);
            return;
        }

        let p = docStatsInflight.get(cacheKey);
        if (!p) {
            p = (async () => {
                const stats = await computeDocumentTextStatsFromFile(fileKey);
                return { ...stats, computedAt: new Date().toISOString() };
            })().finally(() => {
                docStatsInflight.delete(cacheKey);
            });
            docStatsInflight.set(cacheKey, p);
        }

        const value = await p;
        docStatsCache.set(cacheKey, { value, expiresAt: now + DOC_STATS_CACHE_TTL_MS });
        res.json({ ...value, cached: false } satisfies DocumentTextStatsResponse);
    } catch (e) {
        console.error('[documents/stats] failed', e);
        res.status(500).json({ message: 'Server error' });
    }
};

export const updateReadPosition = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;
        const { lastReadPage } = req.body;

        const { data: updatedRows, error } = await supabase
            .from('documents')
            .update({ last_read_page: lastReadPage })
            .eq('id', id)
            .eq('owner_id', req.user?.id)
            .select('id');

        if (error) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        if (!updatedRows || updatedRows.length === 0) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        res.json({ status: 'ok' });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const getDocumentPage = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id, pageNumber } = req.params;
        const pageNum = parseInt(pageNumber);

        const { data: doc, error } = await supabase
            .from('documents')
            .select('id,page_count,file_key,title')
            .eq('id', id)
            .eq('owner_id', req.user?.id)
            .maybeSingle();

        if (error || !doc) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        const format = detectFormat(doc.title, doc.file_key);
        if (format !== 'PDF') {
            res.status(400).json({ message: 'Page extraction is only supported for PDF documents.' });
            return;
        }

        if (pageNum < 1 || pageNum > doc.page_count) {
            res.status(400).json({ message: 'Invalid page number' });
            return;
        }

        if (!fs.existsSync(doc.file_key)) {
            console.error(`File not found at path: ${doc.file_key}`);
            res.status(500).json({ message: `File not found on server: ${doc.file_key}` });
            return;
        }

        const fileBuffer = fs.readFileSync(doc.file_key);
        const pdfDoc = await PDFDocument.load(fileBuffer);

        // Create a new document for just this page
        const newPdf = await PDFDocument.create();
        const [copiedPage] = await newPdf.copyPages(pdfDoc, [pageNum - 1]);
        newPdf.addPage(copiedPage);

        const pdfBytes = await newPdf.save();

        // Extract text from this single page PDF
        // @ts-ignore
        const pdfParse = require('pdf-parse');
        const data = await pdfParse(Buffer.from(pdfBytes));

        console.log(`Extracted text length for page ${pageNum}: ${data.text.length}`);
        console.log('First 100 chars:', data.text.substring(0, 100));

        // Clean up text (optional)
        const cleanText = data.text.trim();

        res.json({
            documentId: doc.id,
            page: pageNum,
            content: cleanText || '(No text found on this page)'
        });

    } catch (error: any) {
        console.error('Error extracting text:', error);
        res.status(500).json({ message: `Server error: ${error.message}` });
    }
};

export const getDocumentFile = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;
        const { data: doc, error } = await supabase
            .from('documents')
            .select('id,file_key,title')
            .eq('id', id)
            .eq('owner_id', req.user?.id)
            .maybeSingle();

        if (error || !doc) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        if (!fs.existsSync(doc.file_key)) {
            res.status(404).json({ message: 'File not found on server' });
            return;
        }

        const format = detectFormat(doc.title, doc.file_key);
        const contentType = mimeTypeFromFormat(format);
        const stat = fs.statSync(doc.file_key);
        const fileSize = stat.size;

        // Enable HTTP range requests for large PDFs (pdf.js streaming) and better performance on mobile.
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Content-Type', contentType);

        const range = req.headers.range;
        if (range) {
            const match = /^bytes=(\d*)-(\d*)$/i.exec(String(range));
            if (!match) {
                res.status(416);
                res.setHeader('Content-Range', `bytes */${fileSize}`);
                res.end();
                return;
            }

            const startRaw = match[1];
            const endRaw = match[2];
            let start = startRaw ? parseInt(startRaw, 10) : NaN;
            let end = endRaw ? parseInt(endRaw, 10) : NaN;

            // Suffix range: "bytes=-500" (last 500 bytes)
            if (!startRaw && endRaw) {
                const suffix = parseInt(endRaw, 10);
                if (Number.isFinite(suffix) && suffix > 0) {
                    start = Math.max(0, fileSize - suffix);
                    end = fileSize - 1;
                }
            }

            if (!Number.isFinite(start)) start = 0;
            if (!Number.isFinite(end)) end = fileSize - 1;

            if (start < 0 || end < start || start >= fileSize) {
                res.status(416);
                res.setHeader('Content-Range', `bytes */${fileSize}`);
                res.end();
                return;
            }

            end = Math.min(end, fileSize - 1);
            const chunkSize = end - start + 1;

            res.status(206);
            res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
            res.setHeader('Content-Length', String(chunkSize));

            const stream = fs.createReadStream(doc.file_key, { start, end });
            stream.on('error', (err) => {
                console.error('File stream error (range)', err);
                try {
                    res.end();
                } catch {}
            });
            stream.pipe(res);
            return;
        }

        // Full file
        res.setHeader('Content-Length', String(fileSize));
        const stream = fs.createReadStream(doc.file_key);
        stream.on('error', (err) => {
            console.error('File stream error', err);
            res.status(500).end();
        });
        stream.pipe(res);
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Server error' });
    }
};

export const getPDFViewer = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;

        // Simplified Viewer HTML
        const viewerHtml = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
  <script src="/api/assets/pdf.min.js"></script>
  <script>
    pdfjsLib.GlobalWorkerOptions.workerSrc = '/api/assets/pdf.worker.min.js';
  </script>
  <style>
    body { margin: 0; padding: 0; background-color: #F9F7F1; font-family: sans-serif; overflow: hidden; } /* Disable body scroll */
    #viewer-container { 
        position: absolute; 
        width: 100%; 
        height: 100%; 
        overflow: hidden; /* Disable container scroll */
        display: flex;
        justify-content: center;
        align-items: flex-start; /* Align to top */
        padding-top: 10px;
    }
    .page { 
        position: relative; 
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); 
        background-color: white; 
        display: none; /* Hide by default */
    }
    .page.active {
        display: block; /* Show only active */
    }
    .textLayer { position: absolute; left: 0; top: 0; right: 0; bottom: 0; overflow: hidden; opacity: 0.2; line-height: 1.0; }
    .textLayer > span { color: transparent; position: absolute; white-space: pre; cursor: pointer; transform-origin: 0% 0%; }
    .textLayer > span:hover { background-color: rgba(165, 216, 255, 0.4); border-radius: 3px; } /* Light Blue Highlight */
  </style>
</head>
<body>
  <div id="viewer-container">
    <div id="viewer" class="pdfViewer"></div>
  </div>

  <script>
    const url = '/api/documents/${id}/file'; 
    let pdfDoc = null;
    let pageRendering = {};
    let baseScale = 1.0;
    let currentPage = 1; 

    async function loadPDF() {
      try {
        const params = new URLSearchParams(window.location.search);
        const token = params.get('token');
        const headers = token ? { 'Authorization': 'Bearer ' + token } : {};
        
        const response = await fetch(url, { headers });
        if (!response.ok) throw new Error('Failed to load PDF');
        
        const blob = await response.blob();
        const blobUrl = URL.createObjectURL(blob);

        pdfDoc = await pdfjsLib.getDocument(blobUrl).promise;
        const viewer = document.getElementById('viewer');

        // Notify React Native about total pages
        if (window.ReactNativeWebView) {
            window.ReactNativeWebView.postMessage(JSON.stringify({ type: 'pdfLoaded', totalPages: pdfDoc.numPages }));
        }

        // 1. Calculate Base Scale (Fit Width)
        const firstPage = await pdfDoc.getPage(1);
        const unscaledViewport = firstPage.getViewport({ scale: 1 });
        const containerWidth = window.innerWidth - 20; 
        baseScale = containerWidth / unscaledViewport.width;

        // 2. Create Page Placeholders
        for (let pageNum = 1; pageNum <= pdfDoc.numPages; pageNum++) {
          const viewport = firstPage.getViewport({ scale: baseScale });

          const pageDiv = document.createElement('div');
          pageDiv.className = 'page';
          if (pageNum === 1) pageDiv.classList.add('active'); // Show first page
          pageDiv.id = 'page-' + pageNum;
          pageDiv.dataset.pageNum = pageNum;
          pageDiv.style.width = viewport.width + 'px';
          pageDiv.style.height = viewport.height + 'px';
          
          // Wrapper for Canvas
          const canvasWrapper = document.createElement('div');
          canvasWrapper.className = 'canvasWrapper';
          canvasWrapper.style.width = '100%';
          canvasWrapper.style.height = '100%';
          canvasWrapper.style.position = 'relative';
          pageDiv.appendChild(canvasWrapper);

          // Text Layer Container
          const textLayerDiv = document.createElement('div');
          textLayerDiv.className = 'textLayer';
          textLayerDiv.style.width = '100%';
          textLayerDiv.style.height = '100%';
          pageDiv.appendChild(textLayerDiv);

          viewer.appendChild(pageDiv);
        }

        // Initial Render
        renderPage(1);

        // 3. Setup Zoom Listener
        let zoomTimeout;
        if (window.visualViewport) {
            window.visualViewport.addEventListener('resize', () => {
                clearTimeout(zoomTimeout);
                zoomTimeout = setTimeout(() => {
                    renderPage(currentPage); // Re-render current page
                }, 300); 
            });
        }

      } catch (error) {
        console.error(error);
        document.body.innerHTML = '<div style="padding:20px;text-align:center;color:red">Error loading PDF</div>';
      }
    }

    async function renderPage(pageNum) {
        if (pageRendering[pageNum]) return;
        
        const pageDiv = document.querySelector('.page[data-page-num="' + pageNum + '"]');
        if (!pageDiv) return;
        
        const wrapper = pageDiv.querySelector('.canvasWrapper');
        const textLayer = pageDiv.querySelector('.textLayer');
        
        const vvScale = window.visualViewport ? window.visualViewport.scale : 1;
        const dpr = window.devicePixelRatio || 1;
        const targetScale = baseScale * vvScale; 
        
        const existingCanvas = wrapper.querySelector('canvas');
        if (existingCanvas) {
            const renderedScale = parseFloat(existingCanvas.dataset.renderedScale || 0);
            if (renderedScale >= targetScale * 0.8) return;
        }

        pageRendering[pageNum] = true;

        try {
            const page = await pdfDoc.getPage(pageNum);
            
            const canvas = document.createElement('canvas');
            const context = canvas.getContext('2d');
            const viewport = page.getViewport({ scale: targetScale });
            
            canvas.width = Math.floor(viewport.width * dpr);
            canvas.height = Math.floor(viewport.height * dpr);
            canvas.style.width = '100%';
            canvas.style.height = '100%';
            canvas.dataset.renderedScale = targetScale;

            const transform = dpr !== 1 ? [dpr, 0, 0, dpr, 0, 0] : null;

            await page.render({
                canvasContext: context,
                viewport: viewport,
                transform: transform
            }).promise;

            wrapper.innerHTML = '';
            wrapper.appendChild(canvas);

            if (textLayer.childElementCount === 0) {
                const textContent = await page.getTextContent();
                const baseViewport = page.getViewport({ scale: baseScale });
                pdfjsLib.renderTextLayer({
                    textContentSource: textContent,
                    container: textLayer,
                    viewport: baseViewport,
                    textDivs: []
                });
            }

        } catch (err) {
            console.error("Render error page " + pageNum, err);
        } finally {
            pageRendering[pageNum] = false;
        }
    }

    // Add click listener for words
    document.addEventListener('click', function(e) {
        if (e.target.tagName === 'SPAN') {
            const word = e.target.textContent.trim();
            if (window.ReactNativeWebView) {
                window.ReactNativeWebView.postMessage(JSON.stringify({ type: 'word', word: word }));
            }
        }
    });

    // Listen for messages from React Native
    document.addEventListener('message', function(event) {
        handleRNMessage(event);
    });
    window.addEventListener('message', function(event) {
        handleRNMessage(event);
    });

    function handleRNMessage(event) {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'nextPage') {
                changePage(currentPage + 1);
            } else if (data.type === 'prevPage') {
                changePage(currentPage - 1);
            } else if (data.type === 'gotoPage') {
                changePage(data.page);
            }
        } catch (e) {
            console.error("Error parsing message", e);
        }
    }

    function changePage(targetPage) {
        targetPage = parseInt(targetPage);
        if (isNaN(targetPage)) return;
        if (targetPage < 1) targetPage = 1;
        if (pdfDoc && targetPage > pdfDoc.numPages) targetPage = pdfDoc.numPages;

        if (targetPage === currentPage) return;

        // Hide current
        const currentEl = document.getElementById('page-' + currentPage);
        if (currentEl) currentEl.classList.remove('active');

        // Show new
        currentPage = targetPage;
        const nextEl = document.getElementById('page-' + currentPage);
        if (nextEl) nextEl.classList.add('active');

        // Render
        renderPage(currentPage);

        // Notify App
        if (window.ReactNativeWebView) {
            window.ReactNativeWebView.postMessage(JSON.stringify({ type: 'pageChanged', page: currentPage }));
        }
        
        // Reset scroll to top of container just in case
        window.scrollTo(0, 0);
    }

    loadPDF();
  </script>
</body>
</html>
        `;

        res.send(viewerHtml);
    } catch (error) {
        res.status(500).send('Error generating viewer');
    }
};

export const deleteDocument = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;
        const { data: doc, error } = await supabase
            .from('documents')
            .select('id,file_key')
            .eq('id', id)
            .eq('owner_id', req.user?.id)
            .maybeSingle();

        if (error || !doc) {
            res.status(404).json({ message: 'Document not found' });
            return;
        }

        // Delete file from filesystem
        if (fs.existsSync(doc.file_key)) {
            try {
                fs.unlinkSync(doc.file_key);
            } catch (err) {
                console.error('Error deleting file:', err);
                // Continue to delete DB record even if file delete fails (orphan file is better than zombie record)
            }
        }

        const { error: deleteError } = await supabase
            .from('documents')
            .delete()
            .eq('id', id)
            .eq('owner_id', req.user?.id);

        if (deleteError) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        res.json({ message: 'Document deleted successfully' });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Server error' });
    }
};
