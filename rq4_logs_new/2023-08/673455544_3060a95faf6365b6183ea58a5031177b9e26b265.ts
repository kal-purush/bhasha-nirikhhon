
export interface IDefaultProviderProps {
  children: React.ReactNode;
}

export interface ICommentContext {
	allComments: [] | TListComments;
	newCommentUser: IComment | null;
	setAllComments: React.Dispatch<React.SetStateAction<[] | TListComments>>;
	setNewCommentUser: React.Dispatch<React.SetStateAction<IComment | null>>;
	registerComment: (formData: TCommentRequest) => Promise<void>;
	editeComment: (formData: ICommentUpdate, commentId: string) => Promise<void>;
	deleteComment: (commentId: string) => Promise<void>;
  }

export interface IComment {
  id: string;
  description: string;
  createdAt: Date;
  carId: number;
  userId: number;
}

export type TCommentRequest = Omit<IComment, "id" | "createdAt" | "userId">;

export type TListComments = IComment[]

export interface IImage {
  id: string;
  imgGalery: string;
  carId: string;
}

export interface ICommentUpdate{
	description: string;
}

// export interface TCarResponse extends ICar {
//   images: IImage[] | [];
//   comments: IComment[] | [];
// }