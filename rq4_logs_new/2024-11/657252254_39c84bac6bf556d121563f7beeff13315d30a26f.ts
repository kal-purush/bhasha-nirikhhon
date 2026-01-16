// response
export interface IGetCampaignStatsResponse {
  success: boolean;
  message: string;
  data:    IGetCampaignStatsResponseDatum[];
}

export interface IGetCampaignStatsResponseDatum {
  status: string;
  count:  number;
}