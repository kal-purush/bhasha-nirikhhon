export interface Friend {
  id: number;
  user: {
    id: number;
    pseudo: string;
    created_at: string;
    updated_at: string;
  };
}