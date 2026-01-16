import axios from 'axios';

interface SignupResponse {
  id: number;
  email: string;
  nickname: string;
  profileImageUrl: string | null;
  createdAt: string;
  updatedAt: string;
}

export const signup = async (email: string, nickname: string, password: string): Promise<SignupResponse> => {
  const teamId = '10-2';
  const url = `https://sp-globalnomad-api.vercel.app/${teamId}/users`;

  try {
    const response = await axios.post<SignupResponse>(
      url,
      {
        email,
        nickname,
        password,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
      },
    );
    return response.data;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      // 오류 응답에 따른 에러 처리
      if (error.response?.status === 400) {
        throw new Error('잘못된 요청: 이메일 형식이 올바르지 않습니다.');
      } else if (error.response?.status === 409) {
        throw new Error('중복된 이메일입니다.');
      } else {
        throw new Error('회원가입 실패: 서버 에러');
      }
    } else {
      throw new Error('회원가입 실패: 예상치 못한 에러');
    }
  }
};