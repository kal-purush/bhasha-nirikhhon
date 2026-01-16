import api from './axios_config'; // axios 설정 파일
import { getCookie } from '../utils/cookie';

/**
 * 모든 보드 목록을 가져오는 API
 * @returns {Promise} - 보드 목록 데이터
 */
export const getBoards = async () => {
  try {
    const response = await api.get('/boards');
    return response.data;
  } catch (error) {
    console.error('보드 목록 가져오기 실패:', error);
    throw error;
  }
};

/**
 * 특정 보드의 상세 정보를 가져오는 API
 * @param {number} boardId - 보드 ID
 * @returns {Promise} - 보드 상세 정보 데이터
 */
export const getBoardDetail = async (boardId) => {
  try {
    const response = await api.get(`/boards/${boardId}`);
    return response.data;
  } catch (error) {
    console.error('보드 상세 정보 가져오기 실패:', error);
    throw error;
  }
};

/**
 * 보드를 공유하는 API
 * @param {number} boardId - 보드 ID
 * @returns {Promise} - 공유 링크 데이터
 */
export const shareBoard = async (boardId) => {
  try {
    const response = await api.post('/boards/share', null, {
      params: { board_id: boardId },
      headers: {
        Authorization: `Bearer ${getCookie('access_token')}`, // 인증 토큰 추가
      },
    });
    return response.data;
  } catch (error) {
    console.error('보드 공유 실패:', error);
    throw error;
  }
};