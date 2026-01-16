import axiosInstance from '@api/axios';

export const postAuthCodeToServer = async (socialProvider: string, code: string) => {
  const response = await axiosInstance.post(
    '/auth/login',
    {
      social_provider: socialProvider,
      authorization_code: code,
    },
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
  console.log('Kakao Auth Success: ', response.data);
  // return response.data;

  localStorage.setItem('kakaoToken', response.data.access_token);
};