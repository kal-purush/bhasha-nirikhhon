import axios from 'axios';

const REST_API_KEY = '04de71a6c4f4b2164ede895d02d3e199';
const REDIRECT_URI = 'http://localhost:3000/login';

export const getKakaoAuthUrl = () => {
  const REST_API_KEY = process.env.NEXT_PUBLIC_KAKAO_REST_API_KEY;
  const REDIRECT_URI = process.env.NEXT_PUBLIC_KAKAO_REDIRECT_URI;
  return `https://kauth.kakao.com/oauth/authorize?client_id=${REST_API_KEY}&redirect_uri=${REDIRECT_URI}&response_type=code`;
};

export const fetchKakaoAccessToken = async (code: string) => {
  const TOKEN_URL = 'https://kauth.kakao.com/oauth/token';

  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: REST_API_KEY,
    redirect_uri: REDIRECT_URI,
    code,
  });

  const response = await axios.post(TOKEN_URL, params, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });

  return response.data.access_token;
};

export const fetchKakaoUserInfo = async (accessToken: string) => {
  const USER_INFO_URL = 'https://kapi.kakao.com/v2/user/me';

  const response = await axios.get(USER_INFO_URL, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });

  return response.data;
};