import { Response } from 'express';
import { supabase } from '../config/supabase';
import type { AuthRequest } from '../types/auth';

export const addVocabularyItem = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { word, translation, sourceLanguage, targetLanguage, documentId, page, favorite, status } = req.body;

        const { data: item, error } = await supabase
            .from('vocabulary_items')
            .insert({
                user_id: req.user?.id,
                word,
                translation,
                source_language: sourceLanguage,
                target_language: targetLanguage,
                document_id: documentId ?? null,
                page: page ?? null,
                favorite: favorite ?? false,
                status: status ?? 'to_review',
            })
            .select('id,created_at')
            .single();

        if (error || !item) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        res.status(201).json({
            id: item.id,
            createdAt: item.created_at,
        });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const getVocabulary = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { status, search, page = 1, limit = 50 } = req.query;

        const pageNum = Number(page) || 1;
        const limitNum = Number(limit) || 50;
        const from = (pageNum - 1) * limitNum;
        const to = from + limitNum - 1;

        let query = supabase
            .from('vocabulary_items')
            .select('id,word,translation,favorite,status,created_at', { count: 'exact' })
            .eq('user_id', req.user?.id)
            .order('created_at', { ascending: false });

        if (status) {
            query = query.eq('status', String(status));
        }

        if (search) {
            const s = String(search);
            query = query.or(`word.ilike.%${s}%,translation.ilike.%${s}%`);
        }

        const { data: items, error, count } = await query.range(from, to);

        if (error) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        res.json({
            items: (items ?? []).map((item) => ({
                id: item.id,
                word: item.word,
                translation: item.translation,
                favorite: item.favorite,
                status: item.status,
                createdAt: item.created_at,
            })),
            page: pageNum,
            total: count ?? 0,
        });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const updateVocabularyItem = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;
        const updates = req.body;

        const allowedUpdates: Record<string, any> = {};

        if (updates.word !== undefined) allowedUpdates.word = updates.word;
        if (updates.translation !== undefined) allowedUpdates.translation = updates.translation;
        if (updates.sourceLanguage !== undefined) allowedUpdates.source_language = updates.sourceLanguage;
        if (updates.targetLanguage !== undefined) allowedUpdates.target_language = updates.targetLanguage;
        if (updates.documentId !== undefined) allowedUpdates.document_id = updates.documentId;
        if (updates.page !== undefined) allowedUpdates.page = updates.page;
        if (updates.favorite !== undefined) allowedUpdates.favorite = updates.favorite;
        if (updates.status !== undefined) allowedUpdates.status = updates.status;
        if (updates.notes !== undefined) allowedUpdates.notes = updates.notes;

        const { data: updatedRows, error } = await supabase
            .from('vocabulary_items')
            .update(allowedUpdates)
            .eq('id', id)
            .eq('user_id', req.user?.id)
            .select('id');

        if (error) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        if (!updatedRows || updatedRows.length === 0) {
            res.status(404).json({ message: 'Item not found' });
            return;
        }

        res.json({ status: 'ok' });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const deleteVocabularyItem = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { id } = req.params;
        const { data: deletedRows, error } = await supabase
            .from('vocabulary_items')
            .delete()
            .eq('id', id)
            .eq('user_id', req.user?.id)
            .select('id');

        if (error) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        if (!deletedRows || deletedRows.length === 0) {
            res.status(404).json({ message: 'Item not found' });
            return;
        }

        res.json({ status: 'deleted' });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};
