import { AuthToken } from "../../../../../@types";
import { Draft } from "../../@types";

export interface Methods {
  /**
   * draft に comment を追加
   *
   * @remarks
   * 必要な権限:
   * - exh : 自身が作成した draft の場合のみ追加可能
   * - blogAdmin : 全ての場合において追加可能
   *
   * @throws Error
   * 400: パラメーターに不備がある
   * 403: 該当する draft の閲覧権限がない
   * 404: 該当する draft が存在しない
   *
   * @returns draft の情報
   */
  post: {
    reqHeaders: AuthToken;
    reqBody: { comment: string };
    resBody: Draft;
  };
}