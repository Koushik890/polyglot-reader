import express, { Request, Response } from 'express';
import { translateText } from '../services/translationService';

const router = express.Router();

router.get('/', async (req: Request, res: Response): Promise<void> => {
    try {
        const { word, sourceLang = 'en', targetLang } = req.query;

        if (!word || !targetLang) {
            res.status(400).json({ message: 'Missing word or targetLang' });
            return;
        }

        const result = await translateText(word as string, sourceLang as string, targetLang as string);

        res.json({
            word: word,
            sourceLanguage: sourceLang,
            targetLanguage: targetLang,
            translation: result.translation,
            phonetic: result.phonetic,
            example: result.example,
        });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
});

// Translate longer plain text (e.g., book summaries) without query-string length limits.
router.post('/text', async (req: Request, res: Response): Promise<void> => {
    try {
        const text = typeof (req.body as any)?.text === 'string' ? (req.body as any).text : '';
        const sourceLangRaw = typeof (req.body as any)?.sourceLang === 'string' ? (req.body as any).sourceLang : null;
        const targetLang = typeof (req.body as any)?.targetLang === 'string' ? (req.body as any).targetLang : '';

        if (!text.trim() || !targetLang.trim()) {
            res.status(400).json({ message: 'Missing text or targetLang' });
            return;
        }

        // Keep this bounded for latency/cost and to avoid abusing upstream providers.
        const MAX_CHARS = 4000;
        const safeText = text.length > MAX_CHARS ? text.slice(0, MAX_CHARS) : text;

        const result = await translateText(safeText, sourceLangRaw, targetLang, { onFailure: 'original' });
        res.json({ translation: result.translation });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
});

export default router;
