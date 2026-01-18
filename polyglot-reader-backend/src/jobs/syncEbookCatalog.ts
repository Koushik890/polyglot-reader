import 'dotenv/config';
import { syncEbookCatalogLanguage } from '../controllers/ebookController';

// Keep this aligned with the mobile app's learning language options.
// Current mobile sources:
// - `LibraryScreen.tsx` LEARNING_LANGUAGE_OPTIONS
// - `DashboardHomeScreen.tsx` LEARNING_LANGUAGE_OPTIONS
const APP_LEARNING_LANGS = ['de', 'en', 'es', 'fr', 'it', 'pl', 'ru', 'bn'] as const;

const parseArgValue = (name: string) => {
  const idx = process.argv.findIndex((a) => a === name);
  if (idx >= 0) return process.argv[idx + 1] || '';
  const withEq = process.argv.find((a) => a.startsWith(`${name}=`));
  if (withEq) return withEq.split('=').slice(1).join('=');
  return '';
};

const parseLangList = (raw: string): string[] =>
  String(raw || '')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

async function main() {
  const all = process.argv.includes('--all');
  const lang = parseArgValue('--lang');
  const langs = parseArgValue('--langs');

  const langList = parseLangList(lang);
  const langsList = parseLangList(langs);
  const envList = parseLangList(process.env.EBOOK_CATALOG_LANGS || 'en,de');

  const targets = all ? [...APP_LEARNING_LANGS] : langList.length ? langList : langsList.length ? langsList : envList.length ? envList : ['en'];

  for (const l of targets) {
    console.log(`[sync] Starting ebook catalog sync for "${l}"...`);
    const result = await syncEbookCatalogLanguage(l);
    console.log(`[sync] Done "${l}" (upserted: ${result.upserted})`);
  }
}

main().catch((e) => {
  console.error('ebook catalog sync failed', e);
  process.exit(1);
});

