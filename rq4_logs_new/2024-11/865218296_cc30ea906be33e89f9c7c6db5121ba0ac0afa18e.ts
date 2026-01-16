import axios from 'axios';

// 전체 게시물 조회 (추천탭)
export const getAllPosts = async () => {
  try {
    const accessToken = localStorage.getItem('accesstoken');

    // 토큰이 없으면 에러 처리
    if (!accessToken) {
      throw new Error('Access token is missing');
    }

    const response = await axios.get(`${process.env.NEXT_PUBLIC_API_BASE_URL}/posts/all`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    console.log(response.data.message);

    return response.data.data; 
  } catch (error) {
    console.error('Error fetching posts:', error);
    throw error; 
  }
};