import type { Request } from 'express';

export interface AuthUser {
    id: string;
    email: string;
    settings: Record<string, any> | null;
    auth_providers?: Record<string, any> | null;
    created_at?: string;
    updated_at?: string;
}

export interface AuthRequest extends Request {
    user?: AuthUser;
}

