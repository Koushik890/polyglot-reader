import 'dotenv/config';

import jwt from 'jsonwebtoken';

import { supabase } from '../src/config/supabase';

type CatalogRow = {
  id: string;
  title: string;
  author: string;
  lang_code: string;
  source: string;
  download_url: string;
};

type ImportJobStatus = 'queued' | 'downloading' | 'processing' | 'done' | 'error';

const API_BASE = (process.env.API_BASE_URL || 'http://localhost:3000/api').replace(/\/+$/g, '');
const JWT_SECRET = process.env.JWT_SECRET || 'secret';

const SMOKE_TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS || 12 * 60_000) || 12 * 60_000;
const SMOKE_POLL_MS = Number(process.env.SMOKE_POLL_MS || 650) || 650;
const SMOKE_WIKISOURCE_MAX_BYTES = Number(process.env.SMOKE_WIKISOURCE_MAX_BYTES || 40 * 1024 * 1024) || 40 * 1024 * 1024;
const SMOKE_HEAD_MAX_CANDIDATES = Number(process.env.SMOKE_HEAD_MAX_CANDIDATES || 8) || 8;

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

async function headContentLength(url: string): Promise<number | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(url, {
      method: 'HEAD',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        accept: '*/*',
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
    });
    if (!res.ok) return null;
    const cl = res.headers.get('content-length');
    const n = cl ? parseInt(cl, 10) : NaN;
    return Number.isFinite(n) && n > 0 ? n : null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function checkManyBooksServerAccess(): Promise<{ ok: boolean; status: number; blocked: boolean }> {
  const url = 'https://manybooks.net/opds/genres';
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        accept: 'application/atom+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
    });
    const server = (res.headers.get('server') || '').toLowerCase();
    const blocked = res.status === 403 && server.includes('cloudflare');
    return { ok: res.ok, status: res.status, blocked };
  } catch {
    return { ok: false, status: 0, blocked: false };
  } finally {
    clearTimeout(timer);
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

async function pickCatalogSampleBySource(source: string): Promise<CatalogRow | null> {
  const resp = await supabase
    .from('ebook_catalog_items')
    .select('id,title,author,lang_code,source,download_url,source_popularity,updated_at')
    .eq('source', source)
    .neq('download_url', '')
    .order('source_popularity', { ascending: false })
    .order('updated_at', { ascending: false })
    .limit(25);

  if (resp.error) throw new Error(`Supabase catalog query failed for ${source}: ${resp.error.message}`);
  const rows = Array.isArray(resp.data) ? (resp.data as any[]) : [];
  const candidates = rows
    .map((r) => ({
      id: String(r?.id || ''),
      title: String(r?.title || ''),
      author: String(r?.author || ''),
      lang_code: String(r?.lang_code || ''),
      source: String(r?.source || source),
      download_url: String(r?.download_url || ''),
    }))
    .filter((r) => r.id && r.download_url.trim().length > 0);
  if (!candidates.length) return null;

  // Wikisource exports can be huge; try to pick a smaller one to keep smoke tests fast.
  if (source === 'wikisource') {
    const scoped = candidates.slice(0, Math.max(1, SMOKE_HEAD_MAX_CANDIDATES));
    const sizes: Array<{ idx: number; bytes: number }> = [];
    for (let i = 0; i < scoped.length; i++) {
      const url = scoped[i]?.download_url || '';
      const n = await headContentLength(url);
      if (typeof n === 'number') sizes.push({ idx: i, bytes: n });
    }
    sizes.sort((a, b) => a.bytes - b.bytes);
    const pickedIdx = sizes.find((s) => s.bytes <= SMOKE_WIKISOURCE_MAX_BYTES)?.idx ?? sizes[0]?.idx ?? 0;
    const chosen = scoped[pickedIdx] || candidates[0]!;
    return chosen;
  }

  // Default: pick the top popularity/recency row.
  return candidates[0]!;
}

async function importAndValidate(opts: {
  token: string;
  row: CatalogRow;
}): Promise<{ ok: boolean; source: string; host: string; title: string; format?: string; error?: string }> {
  const downloadUrl = String(opts.row.download_url || '').trim();
  const host = hostOf(downloadUrl);
  const title = String(opts.row.title || '').trim() || '(untitled)';

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
        title: opts.row.title || '',
        author: opts.row.author || '',
      }),
    });

    const jobId = typeof started.body?.jobId === 'string' ? String(started.body.jobId).trim() : '';
    if (!jobId) throw new Error('Missing jobId from import start.');

    const startedAt = Date.now();
    while (true) {
      if (Date.now() - startedAt > SMOKE_TIMEOUT_MS) throw new Error('Timed out waiting for import job.');
      await sleepMs(SMOKE_POLL_MS);

      const statusResp = await jsonFetch(`${API_BASE}/documents/import/${encodeURIComponent(jobId)}`, {
        headers: { ...authHeaders(opts.token), accept: 'application/json' },
      });

      const status = String(statusResp.body?.status || '') as ImportJobStatus;
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
        const contentType = rangeResp.headers.get('content-type') || '';
        if (!contentType) {
          throw new Error('Missing Content-Type on file response');
        }

        // Cleanup: delete doc
        await jsonFetch(`${API_BASE}/documents/${encodeURIComponent(docId)}`, {
          method: 'DELETE',
          headers: { ...authHeaders(opts.token), accept: 'application/json' },
        });

        return { ok: true, source: opts.row.source, host, title, format };
      }

      if (status === 'error') {
        const msg = typeof statusResp.body?.error === 'string' ? statusResp.body.error : 'Import failed';
        throw new Error(msg);
      }
    }
  } catch (e: any) {
    return { ok: false, source: opts.row.source, host, title, error: typeof e?.message === 'string' ? e.message : String(e) };
  }
}

async function main() {
  console.log(`[smoke-import-sources] API_BASE=${API_BASE}`);
  console.log(`[smoke-import-sources] timeoutMs=${SMOKE_TIMEOUT_MS} pollMs=${SMOKE_POLL_MS}`);

  // ManyBooks is often Cloudflare-blocked from servers. Surface this explicitly in test output.
  const mb = await checkManyBooksServerAccess();
  if (mb.blocked) {
    console.log(`[smoke-import-sources] ManyBooks OPDS check: HTTP ${mb.status} (Cloudflare blocked)`);
  } else if (!mb.ok) {
    console.log(`[smoke-import-sources] ManyBooks OPDS check: HTTP ${mb.status || 'ERR'} (unavailable)`);
  } else {
    console.log(`[smoke-import-sources] ManyBooks OPDS check: HTTP ${mb.status} (OK)`);
  }

  const userId = await getAnyUserId();
  const token = jwt.sign({ id: userId }, JWT_SECRET, { expiresIn: '30m' });

  const sources = ['gutendex', 'wikisource', 'standardebooks', 'wolnelektury', 'manybooks'];
  const picked: CatalogRow[] = [];
  for (const s of sources) {
    const row = await pickCatalogSampleBySource(s);
    if (!row) {
      console.log(`[smoke-import-sources] SKIP ${s}: no catalog rows found`);
      continue;
    }
    picked.push(row);
    console.log(`[smoke-import-sources] Picked ${s} (${row.lang_code}) host=${hostOf(row.download_url)} title="${String(row.title).slice(0, 70)}"`);
  }

  const results: Array<{ ok: boolean; source: string; host: string; title: string; format?: string; error?: string }> = [];

  for (const row of picked) {
    console.log(`\n[smoke-import-sources] Importing ${row.source} :: ${hostOf(row.download_url)} :: ${String(row.title).slice(0, 80)}`);
    const r = await importAndValidate({ token, row });
    results.push(r);
    console.log(r.ok ? `[smoke-import-sources] OK (${r.format || 'unknown'})` : `[smoke-import-sources] FAIL: ${r.error}`);
  }

  const okCount = results.filter((r) => r.ok).length;
  const failCount = results.length - okCount;
  console.log(`\n[smoke-import-sources] Done. ok=${okCount} fail=${failCount}`);
  if (failCount) process.exitCode = 1;
}

main().catch((e) => {
  console.error('[smoke-import-sources] fatal', e);
  process.exitCode = 1;
});

