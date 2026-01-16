// services/recommendationApi.ts
import axios from 'axios';
import { RecommendationRequest, RecommendationResponse } from '../types';

const API_URL = "http://localhost:8080";
const apiClient = axios.create({
    baseURL: API_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

interface IRecommendationsAPI {
  proposeTrainingMenu(body: RecommendationRequest): Promise<RecommendationResponse>;
}

const RecommendationsAPI: IRecommendationsAPI = {
  async proposeTrainingMenu(body: RecommendationRequest): Promise<RecommendationResponse> {
    console.log(body);
    const { data } = await apiClient.post<RecommendationResponse>('/recommendations', body);
    return data;
  },
};

export default RecommendationsAPI;