import 'dotenv/config';
import app from './app';
import { syncEbookCatalogLanguage } from './controllers/ebookController';

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);

    // Optional: run periodic DB-backed ebook catalog sync in the API process.
    // Recommended for dev only; in production, prefer an external scheduler/cron that runs `npm run sync:ebooks`.
    if (process.env.EBOOK_CATALOG_AUTO_SYNC === 'true') {
        const langs = String(process.env.EBOOK_CATALOG_LANGS || 'en,de')
            .split(',')
            .map((s) => s.trim().toLowerCase())
            .filter(Boolean);

        const hours = Number(process.env.EBOOK_CATALOG_SYNC_INTERVAL_HOURS || 24);
        const intervalMs = Math.max(1, (Number.isFinite(hours) ? hours : 24)) * 60 * 60 * 1000;

        const run = async () => {
            for (const lang of langs.length ? langs : ['en']) {
                try {
                    console.log(`[auto-sync] syncing ebook catalog for "${lang}"`);
                    const r = await syncEbookCatalogLanguage(lang);
                    console.log(`[auto-sync] done "${lang}" (upserted: ${r.upserted})`);
                } catch (e) {
                    console.warn(`[auto-sync] failed "${lang}"`, e);
                }
            }
        };

        // Kick off once, then repeat.
        run().catch(() => {});
        setInterval(() => run().catch(() => {}), intervalMs);
    }
});
