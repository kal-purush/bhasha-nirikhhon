import { APIResponse } from "@types";
import API from "..";

type GetResponse = {
  amount: number;
  prev_balance: number;
  activity: string;
  note: string;
  user: {
    npm: string;
  };
  created_at: string;
}[];
export type { GetResponse };

export default class BalanceHistoryServices {
  basePath: string = "/balance/history";
  private api: API = new API();

  async get(
    limit: number = 10,
    page: number = 1,
    order_by: string = "desc",
    sort: string = "created_at",
  ) {
    const targetPath = `${this.basePath}?limit=${limit}&page=${page}&order_by=${order_by}&sort=${sort}`;
    const res: APIResponse<GetResponse> = await this.api.GET(targetPath);
    return res;
  }
}