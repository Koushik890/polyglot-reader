type Provider = 'google' | 'libretranslate' | 'mock';
type FailureMode = 'placeholder' | 'original';

export type TranslateOptions = {
    /**
     * What to return if translation fails (provider down/misconfigured).
     * - placeholder: "[Translated xx->yy: ...]" (useful for dev / word lookup)
     * - original: return the original text (useful for UI copy like summaries)
     */
    onFailure?: FailureMode;
};

const getProvider = (): Provider => {
    const explicit = (process.env.TRANSLATION_PROVIDER || '').toLowerCase().trim();
    if (explicit === 'google') return 'google';
    if (explicit === 'libretranslate') return 'libretranslate';

    // Auto-pick based on configured env vars.
    if (process.env.GOOGLE_TRANSLATE_API_KEY) return 'google';
    if (process.env.LIBRETRANSLATE_URL) return 'libretranslate';
    return 'mock';
};

export const isRealTranslationConfigured = () => getProvider() !== 'mock';

const htmlUnescape = (input: string) =>
    input
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>');

const requireFetch = () => {
    const f = (globalThis as any).fetch as undefined | typeof fetch;
    if (!f) {
        throw new Error('Global fetch is not available. Use Node 18+ or add a fetch polyfill.');
    }
    return f;
};

const normalizeLang = (lang: string) => (lang || '').toLowerCase().trim().split(/[-_]/)[0] || 'en';

const isAuto = (lang: string | null | undefined) => {
    const v = typeof lang === 'string' ? lang.trim().toLowerCase() : '';
    return !v || v === 'auto';
};

const translateWithGoogle = async (text: string, sourceLang: string | null, targetLang: string) => {
    const apiKey = process.env.GOOGLE_TRANSLATE_API_KEY;
    if (!apiKey) throw new Error('Missing GOOGLE_TRANSLATE_API_KEY');

    const fetch = requireFetch();
    const url = `https://translation.googleapis.com/language/translate/v2?key=${encodeURIComponent(apiKey)}`;
    const body: any = {
        q: [text],
        target: normalizeLang(targetLang),
        format: 'text',
    };
    // If source is missing/auto, omit it so Google auto-detects.
    if (!isAuto(sourceLang)) body.source = normalizeLang(sourceLang as string);

    const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });

    if (!resp.ok) {
        const raw = await resp.text().catch(() => '');
        throw new Error(`Google Translate API failed (${resp.status}): ${raw.slice(0, 400)}`);
    }

    const json: any = await resp.json();
    const translated = json?.data?.translations?.[0]?.translatedText;
    if (typeof translated !== 'string' || !translated) {
        throw new Error('Google Translate API returned unexpected response');
    }

    return htmlUnescape(translated);
};

const translateWithLibreTranslate = async (text: string, sourceLang: string | null, targetLang: string) => {
    const base = process.env.LIBRETRANSLATE_URL;
    if (!base) throw new Error('Missing LIBRETRANSLATE_URL');

    const fetch = requireFetch();
    const endpoint = base.endsWith('/') ? `${base}translate` : `${base}/translate`;
    const apiKey = process.env.LIBRETRANSLATE_API_KEY;

    const body: any = {
        q: text,
        // LibreTranslate commonly accepts source=auto; keep explicit codes when provided.
        source: isAuto(sourceLang) ? 'auto' : normalizeLang(sourceLang as string),
        target: normalizeLang(targetLang),
        format: 'text',
    };
    if (apiKey) body.api_key = apiKey;

    const resp = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });

    if (!resp.ok) {
        const raw = await resp.text().catch(() => '');
        throw new Error(`LibreTranslate failed (${resp.status}): ${raw.slice(0, 400)}`);
    }

    const json: any = await resp.json();
    const translated = json?.translatedText;
    if (typeof translated !== 'string' || !translated) {
        throw new Error('LibreTranslate returned unexpected response');
    }

    return translated;
};

export const translateText = async (
    text: string,
    sourceLang: string | null | undefined,
    targetLang: string,
    opts?: TranslateOptions
) => {
    const provider = getProvider();
    const to = normalizeLang(targetLang);
    const onFailure: FailureMode = opts?.onFailure === 'original' ? 'original' : 'placeholder';

    const rawFrom = typeof sourceLang === 'string' ? sourceLang : '';
    const from = rawFrom.trim() ? normalizeLang(rawFrom) : null;

    // Short-circuit for same language.
    if (from && from !== 'auto' && from === to) {
        return { translation: text, phonetic: null, example: null };
    }

    try {
        if (provider === 'google') {
            const translation = await translateWithGoogle(text, from, to);
            return { translation, phonetic: null, example: null };
        }
        if (provider === 'libretranslate') {
            const translation = await translateWithLibreTranslate(text, from, to);
            return { translation, phonetic: null, example: null };
        }

        // Mock fallback (dev only)
        console.warn('[translate] Using MOCK translation provider. Configure GOOGLE_TRANSLATE_API_KEY or LIBRETRANSLATE_URL.');
        if (onFailure === 'original') return { translation: text, phonetic: null, example: null };
        return { translation: `[Translated ${from || 'auto'}->${to}: ${text}]`, phonetic: null, example: null };
    } catch (e) {
        console.error('[translate] Failed', e);
        if (onFailure === 'original') {
            // For UI copy, preserve the original instead of showing a noisy placeholder.
            return { translation: text, phonetic: null, example: null };
        }
        // Fail soft: keep UX functional even if provider is down.
        return { translation: `[Translated ${from || 'auto'}->${to}: ${text}]`, phonetic: null, example: null };
    }
};
