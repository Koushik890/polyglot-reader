import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { supabase } from '../config/supabase';
import type { AuthRequest } from '../types/auth';

export const protect = async (req: AuthRequest, res: Response, next: NextFunction): Promise<void> => {
    let token;

    if (req.headers.authorization && req.headers.authorization.startsWith('Bearer')) {
        try {
            token = req.headers.authorization.split(' ')[1];
            const decoded: any = jwt.verify(token, process.env.JWT_SECRET || 'secret');

            const { data: user, error } = await supabase
                .from('users')
                .select('id,email,settings,auth_providers,created_at,updated_at')
                .eq('id', decoded.id)
                .maybeSingle();

            if (error || !user) {
                res.status(401).json({ message: 'Not authorized, token failed' });
                return;
            }

            req.user = user;
            next();
            return;
        } catch (error) {
            res.status(401).json({ message: 'Not authorized, token failed' });
            return;
        }
    }

    if (!token) {
        res.status(401).json({ message: 'Not authorized, no token' });
        return;
    }
};
