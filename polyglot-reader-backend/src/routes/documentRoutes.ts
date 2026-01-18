import express from 'express';
import multer from 'multer';
import path from 'path';
import {
    uploadDocument,
    postImportDocumentFromUrl,
    getImportDocumentJob,
    getDocuments,
    getDocumentById,
    getDocumentTextStats,
    updateReadPosition,
    getDocumentPage,
    getDocumentFile,
    getPDFViewer,
    deleteDocument,
} from '../controllers/documentController';
import { protect } from '../middleware/auth';

const router = express.Router();

const storage = multer.diskStorage({
    destination(req, file, cb) {
        cb(null, 'uploads/');
    },
    filename(req, file, cb) {
        cb(null, `${file.fieldname}-${Date.now()}${path.extname(file.originalname)}`);
    },
});

const upload = multer({
    storage,
    fileFilter: function (req, file, cb) {
        const lowerName = (file.originalname || '').toLowerCase();
        const ext = path.extname(lowerName);
        const isFb2Zip = lowerName.endsWith('.fb2.zip') || lowerName.endsWith('.fbz');

        const allowedExts = new Set([
            '.pdf',
            '.epub',
            '.mobi',
            '.azw',
            '.azw3',
            '.kf8',
            '.fb2',
            '.cbz',
            '.txt',
        ]);

        const ok = isFb2Zip || allowedExts.has(ext);
        if (ok) return cb(null, true);

        cb(new Error('Unsupported file type. Supported: PDF, EPUB, MOBI, AZW/AZW3/KF8, FB2/FBZ, CBZ, TXT.'));
    },
});

// Server-side import: backend downloads and stores the ebook (more reliable than mobile downloading from 3rd-party hosts).
router.post('/import', protect, postImportDocumentFromUrl);
router.get('/import/:jobId', protect, getImportDocumentJob);

router.post('/', protect, upload.single('file'), uploadDocument);
router.get('/', protect, getDocuments);
router.get('/:id', protect, getDocumentById);
router.get('/:id/stats', protect, getDocumentTextStats);
router.patch('/:id/position', protect, updateReadPosition);
router.get('/:id/pages/:pageNumber', protect, getDocumentPage);
router.get('/:id/file', protect, getDocumentFile);
router.get('/:id/viewer', getPDFViewer); // Viewer loads its own data via token, so the page itself can be public or protected. Let's keep it public but require token in URL param for the file fetch.
router.delete('/:id', protect, deleteDocument);

export default router;
