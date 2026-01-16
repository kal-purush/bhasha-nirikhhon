import axios from 'axios';

const API_URL = '/api/comments';

export interface Comment {
  _id: string;
  content: string;
  owner: string;
  postId: string;
  createdAt: string;
  updatedAt: string;
}

class CommentService {
  async getCommentsByPostId(postId: string): Promise<Comment[]> {
    const response = await axios.get(`${API_URL}/post/${postId}`);
    return response.data;
  }

  async createComment(postId: string, content: string): Promise<Comment> {
    const response = await axios.post(API_URL, { postId, content });
    return response.data;
  }

  async updateComment(id: string, content: string): Promise<Comment> {
    const response = await axios.put(`${API_URL}/${id}`, { content });
    return response.data;
  }

  async deleteComment(id: string): Promise<void> {
    await axios.delete(`${API_URL}/${id}`);
  }
}

export default new CommentService();
