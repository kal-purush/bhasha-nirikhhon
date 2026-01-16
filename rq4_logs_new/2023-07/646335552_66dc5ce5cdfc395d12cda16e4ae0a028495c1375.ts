import { User } from '@/types/User';
import { apiRoute } from '@/libs/api';

const api = apiRoute();

export const useApi = () => ({
  validateToken: async (token: string) => {
    const response = await fetch(api + 'users/' + token);
    const data = await response.json();
    return data.data;

    /*const response = await fetch(api + 'users/validateToken', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
    });
    const data = await response.json();
    return data.data;*/
  },
  signin: async (id: string) => {
    const response = await fetch(api + 'users/' + id);
    const data = await response.json();
    return data.data;
  },
  logout: async () => {
    const response = await fetch(api + 'users/logout');
    const data = await response.json();
    return data.data;
  },
  /* getComments: async (page: string) => {
  },
  createComment: async (content: string, author: User, page: string) => {
  },
  deleteComment: async (id: string) => {
  },
  updateComment: async (id: string, content: string) => {
  }, */
});