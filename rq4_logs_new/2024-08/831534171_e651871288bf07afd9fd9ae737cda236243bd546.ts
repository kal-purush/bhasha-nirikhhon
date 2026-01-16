import axiosConfig from "../config/axios";
import { AxiosResponse } from "axios";

export interface Feed {
  feedId: string;
  feedContent: string;
}

export interface GetAllFeedsResponse {
  status: string;
  data: {
    feedId: string;
    feedContent: string;
  }[];
}

type FeedDetail = {
  status: string;
  data: {
    feedId: string;
    feedContent: string;
  };
};

type FeedDetailResponse = {
  feed: FeedDetail;
};

class FeedService {
  async getAllFeeds(): Promise<AxiosResponse<GetAllFeedsResponse, any>> {
    return (await axiosConfig.get)<GetAllFeedsResponse>("/api/feed");
  }

  //   async getFeedById(id: string): Promise<AxiosResponse<FeedDetailResponse>> {
  //     return axiosConfig.get(`/feed/${id}`);
  //   }
}

export default new FeedService();