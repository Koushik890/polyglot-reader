import express from 'express';
import { protect } from '../middleware/auth';
import { getQuoteDaily, getQuotesFeatured } from '../controllers/quoteController';

const router = express.Router();

router.get('/daily', protect, getQuoteDaily);
router.get('/featured', protect, getQuotesFeatured);

export default router;

