import express, { Response } from 'express';
import type { AuthRequest } from '../types/auth';
import { protect } from '../middleware/auth';
import { mistralChat } from '../services/mistralService';
import { mistralAgentRunOnce } from '../services/mistralAgentService';

const router = express.Router();

const normalizeLang = (lang: unknown) => {
  const t = typeof lang === 'string' ? lang.trim().toLowerCase() : '';
  return t.split(/[-_]/)[0] || '';
};

const stripHtml = (input: string) =>
  String(input || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

router.post('/rewrite', protect, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const textRaw = typeof (req.body as any)?.text === 'string' ? (req.body as any).text : '';
    const targetLangRaw = typeof (req.body as any)?.targetLang === 'string' ? (req.body as any).targetLang : '';
    const sourceLangRaw = typeof (req.body as any)?.sourceLang === 'string' ? (req.body as any).sourceLang : '';
    const title = typeof (req.body as any)?.title === 'string' ? (req.body as any).title : '';
    const author = typeof (req.body as any)?.author === 'string' ? (req.body as any).author : '';

    const targetLang = normalizeLang(targetLangRaw);
    const sourceLang = normalizeLang(sourceLangRaw);

    if (!textRaw.trim() || !targetLang) {
      res.status(400).json({ message: 'Missing text or targetLang' });
      return;
    }

    // Keep bounded for latency/cost.
    const MAX_CHARS = 7000;
    const cleanedInput = stripHtml(textRaw);
    const safeInput = cleanedInput.length > MAX_CHARS ? cleanedInput.slice(0, MAX_CHARS) : cleanedInput;

    // If the caller already wants the same language and the text is short, just return a short paragraph.
    // (We still run through the rewrite prompt to enforce "single paragraph" output.)
    const system = [
      'You rewrite book descriptions into a single short paragraph summary for language learners.',
      '',
      'Rules:',
      '- Output MUST be exactly one paragraph (no bullet points, no headings, no line breaks).',
      '- 2–3 sentences. Aim ~45–70 words (max 85).',
      '- No spoilers (avoid major plot reveals and endings).',
      '- Use natural, fluent language.',
      '- If the source text is not in the output language, translate while summarizing (do not mention translation).',
      '- Do NOT include the book title or author in the output (the app displays it separately).',
      '- Do NOT use Markdown or formatting characters (no *, **, _, backticks). Return plain text only.',
      '- Do NOT mention that this is AI-generated.',
      '- If the input is not enough, write a cautious high-level summary without inventing facts.',
      '',
      `Output language: ${targetLang} (language code).`,
    ].join('\n');

    const user = [
      title || author ? `Book: ${title || 'Unknown title'}${author ? ` — ${author}` : ''}` : '',
      sourceLang ? `Source language hint: ${sourceLang}` : '',
      '',
      'Source text:',
      safeInput,
    ]
      .filter((x) => String(x).trim().length > 0)
      .join('\n');

    // Prefer a configured Mistral Agent when available (Mistral Agents API).
    let out: string | null = null;
    let provider: 'mistral_agent' | 'mistral_chat' | null = null;
    const agentId = (process.env.MISTRAL_AGENT_ID || '').trim();
    if (agentId) {
      try {
        out = await mistralAgentRunOnce({
          agentId,
          prompt: `${system}\n\n${user}`,
          store: false,
        });
        if (out) provider = 'mistral_agent';
      } catch (e) {
        console.warn('[summaries] Mistral agent run failed; falling back', (e as any)?.message || e);
        out = null;
      }
    }

    // Fallback: direct chat completions (non-agent) if configured.
    try {
      if (!out) {
        out = await mistralChat(
          [
            { role: 'system', content: system },
            { role: 'user', content: user },
          ],
          { max_tokens: 200, temperature: 0.5, top_p: 1 }
        );
        if (out) provider = 'mistral_chat';
      }
    } catch (e) {
      console.warn('[summaries] Mistral rewrite unavailable', (e as any)?.message || e);
      out = null;
    }

    if (!out) {
      res.status(503).json({ message: 'Summary rewrite unavailable (Mistral not configured or request failed)' });
      return;
    }

    // Final cleanup: keep it one paragraph.
    const normalized = out.replace(/\s+/g, ' ').trim();
    res.json({ summary: normalized, provider });
  } catch (e) {
    console.error('[summaries] rewrite failed', e);
    res.status(500).json({ message: 'Server error' });
  }
});

export default router;

