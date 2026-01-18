import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import fs from 'fs';
import type { Request } from 'express';

import authRoutes from './routes/authRoutes';
import documentRoutes from './routes/documentRoutes';
import translateRoutes from './routes/translateRoutes';
import summaryRoutes from './routes/summaryRoutes';
import vocabularyRoutes from './routes/vocabularyRoutes';
import userRoutes from './routes/userRoutes';
import quoteRoutes from './routes/quoteRoutes';
import ebookRoutes from './routes/ebookRoutes';

import path from 'path';

const app = express();

// Disable CSP for now to allow inline scripts in the PDF viewer
app.use(helmet({
  contentSecurityPolicy: false,
  // Our mobile app loads viewer JS (PDF.js + foliate-js) from the API domain inside a WebView (about:blank origin).
  // Use cross-origin so the browser doesn't block these static assets.
  crossOriginResourcePolicy: { policy: 'cross-origin' },
}));
// WebViews load PDFs + PDF.js assets cross-origin (about:blank origin), and pdf.js needs access
// to range/length headers to enable streaming for large documents.
app.use(
  cors({
    exposedHeaders: ['Accept-Ranges', 'Content-Range', 'Content-Length'],
  })
);
app.use(morgan('dev'));
app.use(express.json());

// Serve static assets (PDF.js + vendored foliate-js modules for multi-format reader)
// In dev (ts-node), assets live under src/assets. In production builds, we copy them to dist/assets.
const distAssetsDir = path.join(__dirname, 'assets');
const srcAssetsDir = path.join(process.cwd(), 'src', 'assets');
const assetsDir = fs.existsSync(distAssetsDir) ? distAssetsDir : srcAssetsDir;

// Mobile app can cache foliate-js modules for offline reading.
app.get('/api/vendor/foliate-manifest', (req: Request, res) => {
  try {
    const foliateRoot = path.join(assetsDir, 'foliate');
    if (!fs.existsSync(foliateRoot)) {
      res.status(404).json({ message: 'Foliate assets not found' });
      return;
    }

    const files: string[] = [];
    const walk = (dir: string, relBase: string) => {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const e of entries) {
        const abs = path.join(dir, e.name);
        const rel = relBase ? path.posix.join(relBase, e.name) : e.name;
        if (e.isDirectory()) {
          walk(abs, rel);
        } else if (e.isFile()) {
          // Only ship runtime JS modules.
          if (rel.toLowerCase().endsWith('.js')) files.push(rel);
        }
      }
    };

    walk(foliateRoot, '');
    files.sort((a, b) => a.localeCompare(b));

    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.json({ generatedAt: new Date().toISOString(), files });
  } catch (e) {
    console.error('foliate-manifest failed', e);
    res.status(500).json({ message: 'Server error' });
  }
});
app.use('/api/assets', express.static(assetsDir));
app.use('/api/vendor/foliate', express.static(path.join(assetsDir, 'foliate')));

app.use('/api/auth', authRoutes);
app.use('/api/documents', documentRoutes);
app.use('/api/translate', translateRoutes);
app.use('/api/summaries', summaryRoutes);
app.use('/api/quotes', quoteRoutes);
app.use('/api/vocabulary', vocabularyRoutes);
app.use('/api/users', userRoutes);
app.use('/api/ebooks', ebookRoutes);

app.get('/', (req, res) => {
  res.json({ message: 'Polyglot Reader API' });
});

export default app;
