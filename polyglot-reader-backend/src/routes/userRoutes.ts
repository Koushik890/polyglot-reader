import express from 'express';
import { getMe, updateSettings } from '../controllers/userController';
import { protect } from '../middleware/auth';

const router = express.Router();

router.get('/me', protect, getMe);
router.patch('/me/settings', protect, updateSettings);

export default router;
