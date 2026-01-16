import axios, { AxiosResponse } from "axios";
import { Comment } from '../types/comment.interface';
import { APIService } from "../store/service";

class CommentService extends APIService {
  getComments = (): Promise<AxiosResponse<Comment[]>> =>
    axios.get<Comment[]>(`${this.apiUrl}/comments`);

  getPostComments = (postId: number): Promise<AxiosResponse<Comment[]>> =>
    axios.get<Comment[]>(`${this.apiUrl}/comments?postId=${postId}`);

  deleteComment = (id: number): Promise<AxiosResponse<Comment>> =>
    axios.delete<Comment>(`${this.apiUrl}/comments/${id}`);
}

export default new CommentService('https://jsonplaceholder.typicode.com');