import express from 'express';
import {
  getEbookCatalogStatus,
  getEbooks,
  getEbooksAuthor,
  getEbooksCategories,
  getEbooksCategory,
  getEbooksSearch,
  postEbookCatalogImport,
  postEbookCatalogSync,
} from '../controllers/ebookController';
import { postEbookTextStats } from '../controllers/ebookStatsController';
import { protect } from '../middleware/auth';

const router = express.Router();

// Returns free, non-academic ebooks filtered by learning language.
router.get('/catalog/status', protect, getEbookCatalogStatus);
router.post('/catalog/import', protect, postEbookCatalogImport);
router.post('/catalog/sync', protect, postEbookCatalogSync);
router.post('/stats', protect, postEbookTextStats);
router.get('/search', protect, getEbooksSearch);
router.get('/author', protect, getEbooksAuthor);
router.get('/categories', protect, getEbooksCategories);
router.get('/category', protect, getEbooksCategory);
router.get('/', protect, getEbooks);

export default router;

