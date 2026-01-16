import { UserInfo } from "apis/auth/@types";

export interface Draft {
  id: number;
  author: UserInfo;
  content: string;
  status: DraftStatus;
  comments: DraftComment[];
  created_at: string;
}

export type NewDraft = Pick<Draft, "content">;

export interface DraftComment {
  author: UserInfo;
  message: string;
}

/**
 * draft の状態
 *
 * @remarks
 * - `waiting` : admin の審査待ち
 * - `accepted`: 承認済 この状態の draft のみ公開可能
 * - `rejected`: 却下済
 * - `published`: 公開済
 */
export type DraftStatus = "waiting" | "accepted" | "rejected" | "published";