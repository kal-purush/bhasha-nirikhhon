import jwt from 'jsonwebtoken';
import { NextApiRequest, NextApiResponse } from 'next';

const JWT_SECRET = 'your-secret-key'; // 반드시 환경 변수로 관리하세요

// JWT 토큰 생성
export const generateToken = (payload: object): string => {
    return jwt.sign(payload, JWT_SECRET, { expiresIn: '1h' });
};

// JWT 토큰 검증
export const verifyToken = (token: string): object | null => {
    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        return decoded as object;
    } catch (err) {
        console.error('토큰 검증 실패:', err);
        return null;
    }
};

// 인증 미들웨어
export const authMiddleware = (handler: Function) => {
    return async (req: NextApiRequest, res: NextApiResponse) => {
        const token = req.headers.authorization?.split(' ')[1];

        if (!token) {
            res.status(401).json({ message: '인증이 필요합니다.' });
            return;
        }

        const decoded = verifyToken(token);
        if (!decoded) {
            res.status(401).json({ message: '잘못된 토큰입니다.' });
            return;
        }

        (req as any).user = decoded;
        return handler(req, res);
    };
};