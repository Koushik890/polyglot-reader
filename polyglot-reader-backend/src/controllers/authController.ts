import { Request, Response } from 'express';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { supabase } from '../config/supabase';

const generateToken = (id: string) => {
    return jwt.sign({ id }, process.env.JWT_SECRET || 'secret', {
        expiresIn: '30d',
    });
};

export const signup = async (req: Request, res: Response): Promise<void> => {
    try {
        console.log('Signup request received:', req.body);
        const { email, password } = req.body;

        if (!email || !password) {
            res.status(400).json({ message: 'Please provide email and password' });
            return;
        }

        const { data: existingUsers, error: existingError } = await supabase
            .from('users')
            .select('id')
            .eq('email', email)
            .limit(1);

        if (existingError) {
            res.status(500).json({ message: 'Server error' });
            return;
        }

        if (existingUsers && existingUsers.length > 0) {
            res.status(400).json({ message: 'User already exists' });
            return;
        }

        const salt = await bcrypt.genSalt(10);
        const passwordHash = await bcrypt.hash(password, salt);

        const { data: user, error: createError } = await supabase
            .from('users')
            .insert({
                email,
                password_hash: passwordHash,
            })
            .select('id,email,settings')
            .single();

        if (createError || !user) {
            // Handle unique constraint violation just in case
            const msg = createError?.message?.toLowerCase().includes('duplicate')
                ? 'User already exists'
                : 'Invalid user data';
            res.status(400).json({ message: msg });
            return;
        }

        res.status(201).json({
            token: generateToken(user.id),
            user: {
                id: user.id,
                email: user.email,
                settings: user.settings,
            },
        });
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};

export const login = async (req: Request, res: Response): Promise<void> => {
    try {
        const { email, password } = req.body;

        const { data: user, error: userError } = await supabase
            .from('users')
            .select('id,email,password_hash,settings')
            .eq('email', email)
            .maybeSingle();

        if (userError || !user) {
            res.status(401).json({ message: 'Invalid email or password' });
            return;
        }

        if (await bcrypt.compare(password, user.password_hash)) {
            res.json({
                token: generateToken(user.id),
                user: {
                    id: user.id,
                    email: user.email,
                    settings: user.settings,
                },
            });
        } else {
            res.status(401).json({ message: 'Invalid email or password' });
        }
    } catch (error) {
        res.status(500).json({ message: 'Server error' });
    }
};
