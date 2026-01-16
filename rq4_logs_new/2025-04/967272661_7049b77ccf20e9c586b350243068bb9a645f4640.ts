type Frequency = 'DAILY' | 'WEEKLY' | 'ONCE' | 'MONTHLY';

export interface TaskListItemResponse {
  doneBy: {
    user: {
      image: string;
      nickname: string;
      id: number;
    };
  };
  writer: {
    image: string;
    nickname: string;
    id: number;
  };
  displayIndex: number;
  commentCount: number;
  deletedAt: string;
  recurringId: number;
  frequency: Frequency;
  updatedAt: string;
  doneAt: string | null;
  date: string;
  description: string;
  name: string;
  id: number;
}

export interface TaskListItemType {
  commentCount: number;
  date: string;
  frequency: Frequency;
  doneAt: string | null;
  description: string;
}