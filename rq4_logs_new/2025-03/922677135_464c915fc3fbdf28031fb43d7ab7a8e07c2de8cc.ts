import axios from 'axios';

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080/similar';

// 유사 웹툰 등록
export const createSimilar = async (
  targetWebtoonId: number,
  choiceWebtoonId: number
) => {
  try {
    const response = await axios.post(
      `${API_BASE_URL}`,
      { targetWebtoonId, choiceWebtoonId },
      {
        withCredentials: true,
      }
    );
    return response.data;
  } catch (error) {
    console.error('유사 웹툰 등록 실패:', error);
    return null;
  }
};

// 유사 웹툰 삭제
export const deleteSimilar = async (similarId: number) => {
  try {
    const response = await axios.delete(`${API_BASE_URL}/${similarId}`, {
      withCredentials: true,
    });
    return response.status === 200;
  } catch (error) {
    console.error('유사 웹툰 삭제 실패:', error);
    return false;
  }
};

// 유사 웹툰 목록 조회 (페이징 지원)
export const getSimilarList = async (
  targetWebtoonId: number,
  page = 0,
  size = 10
) => {
  try {
    const response = await axios.get(`${API_BASE_URL}`, {
      params: { targetWebtoonId, page, size },
    });
    return response.data;
  } catch (error) {
    console.error('유사 웹툰 목록 가져오기 실패:', error);
    return null;
  }
};