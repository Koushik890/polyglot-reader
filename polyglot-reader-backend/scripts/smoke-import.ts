import 'dotenv/config';

import jwt from 'jsonwebtoken';

import { supabase } from '../src/config/supabase';

type ApiEbook = {
  id: string;
  title: string;
  author: string;
  category?: string;
  coverUrl?: string | null;
  downloadUrl?: string | null;
};

type ImportJobStatus = 'queued' | 'downloading' | 'processing' | 'done' | 'error';

const API_BASE = (process.env.API_BASE_URL || 'http://localhost:3000/api').replace(/\/+$/g, '');
const JWT_SECRET = process.env.JWT_SECRET || 'secret';

const SMOKE_LANG = (process.env.SMOKE_LANG || 'de').trim().toLowerCase();
const SMOKE_TRENDING_LIMIT = Number(process.env.SMOKE_TRENDING_LIMIT || 24) || 24;
const SMOKE_MAX_IMPORTS = Number(process.env.SMOKE_MAX_IMPORTS || 3) || 3;
const SMOKE_TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS || 6 * 60_000) || 6 * 60_000;
const SMOKE_POLL_MS = Number(process.env.SMOKE_POLL_MS || 650) || 650;

const sleepMs = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, Math.floor(ms || 0))));

const authHeaders = (token: string) => ({
  Authorization: `Bearer ${token}`,
});

async function jsonFetch(url: string, init?: RequestInit) {
  const res = await fetch(url, init);
  const text = await res.text();
  let body: any = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!res.ok) {
    const msg =
      typeof body?.message === 'string'
        ? body.message
        : typeof body === 'string' && body.trim()
          ? body
          : `HTTP ${res.status}`;
    const err: any = new Error(msg);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return { res, body };
}

function hostOf(downloadUrl: string) {
  try {
    return new URL(downloadUrl).hostname.toLowerCase();
  } catch {
    return '';
  }
}

async function getAnyUserId(): Promise<string> {
  const { data, error } = await supabase.from('users').select('id,email').limit(1);
  if (error) throw new Error(`Supabase users query failed: ${error.message}`);
  const first = Array.isArray(data) ? data[0] : null;
  const id = typeof first?.id === 'string' ? first.id : '';
  if (!id) throw new Error('No users found in DB (need at least one user to mint a JWT for smoke tests).');
  return id;
}

async function importAndValidate(opts: {
  token: string;
  ebook: ApiEbook;
}): Promise<{ ok: boolean; host: string; title: string; docId?: string; format?: string; error?: string }> {
  const downloadUrl = String(opts.ebook.downloadUrl || '').trim();
  const host = hostOf(downloadUrl);
  const title = String(opts.ebook.title || '').trim() || '(untitled)';

  try {
    const started = await jsonFetch(`${API_BASE}/documents/import`, {
      method: 'POST',
      headers: {
        ...authHeaders(opts.token),
        'content-type': 'application/json',
        accept: 'application/json',
      },
      body: JSON.stringify({
        downloadUrl,
        title: opts.ebook.title || '',
        author: opts.ebook.author || '',
      }),
    });

    const jobId = typeof started.body?.jobId === 'string' ? String(started.body.jobId).trim() : '';
    if (!jobId) throw new Error('Missing jobId from import start.');

    const startedAt = Date.now();
    let lastProgress = 0;
    while (true) {
      if (Date.now() - startedAt > SMOKE_TIMEOUT_MS) throw new Error('Timed out waiting for import job.');

      await sleepMs(SMOKE_POLL_MS);
      const statusResp = await jsonFetch(`${API_BASE}/documents/import/${encodeURIComponent(jobId)}`, {
        headers: {
          ...authHeaders(opts.token),
          accept: 'application/json',
        },
      });

      const status = String(statusResp.body?.status || '') as ImportJobStatus;
      const p = typeof statusResp.body?.progress === 'number' ? statusResp.body.progress : 0;
      if (p + 1e-6 < lastProgress) {
        console.warn(`[smoke-import] WARN progress decreased for job ${jobId}: ${lastProgress} -> ${p}`);
      }
      lastProgress = Math.max(lastProgress, p);

      if (status === 'done') {
        const doc = statusResp.body?.document || null;
        const docId = typeof doc?.id === 'string' ? doc.id : '';
        const format = typeof doc?.format === 'string' ? doc.format : undefined;
        if (!docId) throw new Error('Import done but document is missing.');

        // Validate file fetch works (and Range works).
        const fileUrl = `${API_BASE}/documents/${encodeURIComponent(docId)}/file`;
        const rangeResp = await fetch(fileUrl, {
          headers: {
            ...authHeaders(opts.token),
            Range: 'bytes=0-1023',
          },
        });
        if (!(rangeResp.status === 206 || rangeResp.status === 200)) {
          throw new Error(`File fetch failed (HTTP ${rangeResp.status})`);
        }
        const acceptRanges = rangeResp.headers.get('accept-ranges') || '';
        const contentRange = rangeResp.headers.get('content-range') || '';
        if (rangeResp.status === 206 && !contentRange) {
          throw new Error('Expected Content-Range header for 206 response.');
        }
        if (!acceptRanges) {
          console.warn('[smoke-import] WARN missing Accept-Ranges header');
        }

        // Cleanup: delete doc
        await jsonFetch(`${API_BASE}/documents/${encodeURIComponent(docId)}`, {
          method: 'DELETE',
          headers: {
            ...authHeaders(opts.token),
            accept: 'application/json',
          },
        });

        return { ok: true, host, title, docId, format };
      }

      if (status === 'error') {
        const msg = typeof statusResp.body?.error === 'string' ? statusResp.body.error : 'Import failed';
        throw new Error(msg);
      }
    }
  } catch (e: any) {
    return { ok: false, host, title, error: typeof e?.message === 'string' ? e.message : String(e) };
  }
}

async function main() {
  console.log(`[smoke-import] API_BASE=${API_BASE}`);
  console.log(`[smoke-import] lang=${SMOKE_LANG} trendingLimit=${SMOKE_TRENDING_LIMIT} maxImports=${SMOKE_MAX_IMPORTS}`);

  const userId = await getAnyUserId();
  const token = jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '30m' });

  const ebooksResp = await jsonFetch(`${API_BASE}/ebooks?lang=${encodeURIComponent(SMOKE_LANG)}&trendingLimit=${encodeURIComponent(String(SMOKE_TRENDING_LIMIT))}`, {
    headers: {
      ...authHeaders(token),
      accept: 'application/json',
    },
  });

  const trending = Array.isArray(ebooksResp.body?.trending) ? (ebooksResp.body.trending as ApiEbook[]) : [];
  if (!trending.length) {
    throw new Error('No trending ebooks returned from /api/ebooks');
  }

  const wantedHosts = ['gutenberg.org', 'ws-export.wmcloud.org', 'standardebooks.org', 'wolnelektury.pl', 'manybooks.net'];
  const selected: ApiEbook[] = [];
  const seen = new Set<string>();

  for (const w of wantedHosts) {
    const found = trending.find((b) => {
      const dl = String(b?.downloadUrl || '').trim();
      if (!dl) return false;
      const h = hostOf(dl);
      return h === w || h.endsWith(`.${w}`);
    });
    if (found) {
      const key = String(found.downloadUrl || '').trim();
      if (key && !seen.has(key)) {
        selected.push(found);
        seen.add(key);
      }
      if (selected.length >= SMOKE_MAX_IMPORTS) break;
    }
  }

  // Fallback: just take first unique items.
  if (!selected.length) {
    for (const b of trending) {
      const dl = String(b?.downloadUrl || '').trim();
      if (!dl || seen.has(dl)) continue;
      selected.push(b);
      seen.add(dl);
      if (selected.length >= SMOKE_MAX_IMPORTS) break;
    }
  }

  console.log(`[smoke-import] Selected ${selected.length} ebook(s) for import tests`);

  const results: Array<{ ok: boolean; host: string; title: string; format?: string; error?: string }> = [];
  for (const ebook of selected) {
    const dl = String(ebook.downloadUrl || '').trim();
    const host = hostOf(dl);
    console.log(`\n[smoke-import] Importing ${host} :: ${String(ebook.title || '').slice(0, 80)}`);
    const r = await importAndValidate({ token, ebook });
    results.push(r);
    console.log(r.ok ? `[smoke-import] OK (${r.format || 'unknown'})` : `[smoke-import] FAIL: ${r.error}`);
  }

  const okCount = results.filter((r) => r.ok).length;
  const failCount = results.length - okCount;
  console.log(`\n[smoke-import] Done. ok=${okCount} fail=${failCount}`);
  if (failCount) {
    process.exitCode = 1;
  }
}

main().catch((e) => {
  console.error('[smoke-import] fatal', e);
  process.exitCode = 1;
});

