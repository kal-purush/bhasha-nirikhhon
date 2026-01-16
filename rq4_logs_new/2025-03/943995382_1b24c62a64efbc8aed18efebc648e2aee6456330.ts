import axios from 'axios';
import { ArticleResponse } from '../types/article';

const API_KEY = process.env.NEXT_PUBLIC_NY_TIMES_API_KEY;
const BASE_URL = 'https://api.nytimes.com/svc/mostpopular/v2';

export const getPopularArticles = async (period: 1 | 7 | 30): Promise<ArticleResponse> => {
  try {
    const response = await axios.get<ArticleResponse>(
      `${BASE_URL}/viewed/${period}.json?api-key=${API_KEY}`
    );
    return response.data;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      throw new Error(error.response?.data?.message || 'Failed to fetch articles');
    }
    throw error;
  }
}; 