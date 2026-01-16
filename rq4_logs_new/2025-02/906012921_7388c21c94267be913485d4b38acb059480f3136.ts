import { User, UserPassWord } from '~/models/User';
import { authClient } from './auth/authClient';

// 유저 정보 수정
export const updateUser = async (user: User): Promise<User> => {
  try {
    console.log('보낼 데이터:', user);
    const response = await authClient.put<User>(`/user/info`, user, {
      headers: {
        'Content-Type': 'application/json',
      },
    });
    console.log('응답 데이터:', response.data);
    return response.data;
  } catch (error) {
    console.log(error);
    throw error;
  }
};

// 유저 삭제
export const deleteUser = async () => {
  try {
    await authClient.delete(`/user`);
  } catch (error) {
    console.log(error);
    throw error;
  }
};

// 유저 비밀번호 변경
export const changeUserPassword = async (): Promise<UserPassWord> => {
  try {
    const response = await authClient.put<UserPassWord>(
      `/user/password-change`,
    );
    return response.data;
  } catch (error) {
    console.log(error);
    throw error;
  }
};

// 유저 프사 변경
export const changeUserProfileImage = async (file: File): Promise<string> => {
  try {
    const formData = new FormData();
    formData.append('image', file);

    // PUT 요청으로 이미지 업로드
    const response = await authClient.put<string>('/user/image', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  } catch (error) {
    console.log(error);
    throw error;
  }
};

// 유저 프사 최종 업로드
export const uploadProfileImage = async (blob: File): Promise<string> => {
  const formData = new FormData();
  formData.append('image', blob);

  try {
    const response = await authClient.post<string>('/image/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    const imageUrl = response.data;

    console.log('받은 이미지 URL:', imageUrl);
    alert('이미지 업로드 성공');

    return imageUrl;
  } catch (error) {
    console.error('이미지 업로드 실패:', error);
    alert('이미지 업로드 실패');
    throw error;
  }
};