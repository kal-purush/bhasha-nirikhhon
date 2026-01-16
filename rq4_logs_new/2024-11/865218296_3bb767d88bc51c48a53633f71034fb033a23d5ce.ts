// src/api/follow.ts
import axios from "axios";

type FollowResponse = {
  code: number;
  message: string;
  data: null;
};

type FollowError = {
  code: number;
  message: string;
  data: null;
};

export const followUser = async (
  userId: string,
  accessToken: string
): Promise<{
  success: boolean;
  message?: string;
  errorCode?: number;
  errorMessage?: string;
}> => {
  try {
    const response = await axios.post<FollowResponse>(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/follows/${userId}`,
      {},
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      }
    );

    console.log("팔로우 요청 성공:", response.data.message);
    return { success: true, message: response.data.message };
  } catch (error: unknown) {
    if (axios.isAxiosError(error) && error.response) {
      const errorData = error.response.data as FollowError;
      const errorCode = errorData.code;
      const errorMessage = errorData.message;

      return { success: false, errorCode, errorMessage };
    }

    console.log("팔로우 요청 중 알 수 없는 오류 발생:", error);
    return { success: false, errorMessage: "네트워크 오류가 발생했습니다." };
  }
};