import express from 'express';
import { addVocabularyItem, getVocabulary, updateVocabularyItem, deleteVocabularyItem } from '../controllers/vocabularyController';
import { protect } from '../middleware/auth';

const router = express.Router();

router.post('/', protect, addVocabularyItem);
router.get('/', protect, getVocabulary);
router.patch('/:id', protect, updateVocabularyItem);
router.delete('/:id', protect, deleteVocabularyItem);

export default router;
