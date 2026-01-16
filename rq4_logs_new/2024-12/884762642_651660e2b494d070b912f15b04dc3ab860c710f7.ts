import axiosInstance from '@api/axios';

type SocialProvider = 'KAKAO' | 'GOOGLE' | 'NAVER';

type AuthResponse = {
  status: number;
  data: {
    access_token: string;
    badge: boolean;
    message: string;
    user_id: string;
    username: null | string;
  };
};

export const postAuthCodeToServer = async (
  socialProvider: SocialProvider,
  code: string,
): Promise<AuthResponse | undefined> => {
  try {
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

    return { status: response.status, data: response.data };
  } catch (error) {
    console.error('Kakao Auth Error:', error);
  }
};