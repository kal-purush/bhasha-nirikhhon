import { NextApiRequest, NextApiResponse } from 'next';
import jwt from 'jsonwebtoken';
import { proxyApi } from '@/services/api';  // 프록시 서버를 통한 토큰 갱신 요청

// JWT 검증 함수
const validateToken = (token: string | undefined) => {
  try {
    if (!token) throw new Error('Token is required');
    return jwt.verify(token, process.env.JWT_SECRET!);  // 토큰 검증
  } catch (error) {
    return null;
  }
};

// 리프레시 토큰을 사용하여 새로운 액세스 토큰을 발급받는 함수
const refreshAccessToken = async (refreshToken: string) => {
  try {
    // 프록시 서버에서 리프레시 토큰을 사용하여 액세스 토큰을 갱신
    const { accessToken } = await proxyApi
      .post('api/auth/refresh', { json: { refreshToken } })
      .json<{ accessToken: string }>();

    return accessToken;
  } catch (error) {
    throw new Error('Failed to refresh access token');
  }
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { token, refreshToken } = req.cookies;

  // 1. 액세스 토큰이 없으면 리프레시 토큰을 사용해 액세스 토큰을 갱신
  if (!token) {
    if (!refreshToken) {
      return res.status(401).json({ message: 'Token and refreshToken are missing' });
    }

    try {
      const newAccessToken = await refreshAccessToken(refreshToken);
      // 새로운 액세스 토큰을 쿠키에 설정
      res.setHeader('Set-Cookie', `accessToken=${newAccessToken}; HttpOnly; Path=/; Max-Age=3600; Secure; SameSite=Strict`);
      return res.status(200).json({ message: 'Access token refreshed', accessToken: newAccessToken });
    } catch (error) {
      return res.status(401).json({ message: 'Failed to refresh access token' });
    }
  }

  // 2. 액세스 토큰이 있을 경우 검증
  const decoded = validateToken(token);

  if (!decoded) {
    if (!refreshToken) {
      return res.status(401).json({ message: 'Invalid or expired access token and no refresh token available' });
    }

    try {
      // 액세스 토큰이 만료되었거나 유효하지 않다면 리프레시 토큰을 통해 갱신
      const newAccessToken = await refreshAccessToken(refreshToken);
      res.setHeader('Set-Cookie', `accessToken=${newAccessToken}; HttpOnly; Path=/; Max-Age=3600; Secure; SameSite=Strict`);
      return res.status(200).json({ message: 'Access token refreshed', accessToken: newAccessToken });
    } catch (error) {
      return res.status(401).json({ message: 'Failed to refresh access token' });
    }
  }

  return res.status(200).json({ message: 'Token is valid', data: decoded });
}