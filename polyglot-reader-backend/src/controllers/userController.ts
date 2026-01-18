import { Response } from 'express';
import { supabase } from '../config/supabase';
import type { AuthRequest } from '../types/auth';

export const getMe = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { data: user, error } = await supabase
            .from('users')
            .select('id,email,settings,auth_providers,created_at,updated_at')
            .eq('id', req.user?.id)
            .maybeSingle();

        if (error || !user) {
            res.status(404).json({ message: 'User not found' });
            return;
        }

        res.json(user);
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const updateSettings = async (req: AuthRequest, res: Response): Promise<void> => {
    try {
        const { data: existing, error: existingError } = await supabase
            .from('users')
            .select('settings')
            .eq('id', req.user?.id)
            .maybeSingle();

        if (existingError || !existing) {
            res.status(404).json({ message: 'User not found' });
            return;
        }

        const nextSettings = {
            ...(existing.settings ?? {}),
            ...(req.body ?? {}),
        };

        const { data: updated, error: updateError } = await supabase
            .from('users')
            .update({ settings: nextSettings })
            .eq('id', req.user?.id)
            .select('settings')
            .single();

        if (updateError || !updated) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        res.json({ status: 'ok', settings: updated.settings });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};
