export interface User {
  id: number;
  name: string;
  email: string;
}

export interface PageProps {
  auth: {
    user: User | null;
  };
  errors: Record<string, string>;
  flash?: {
    success?: string;
    error?: string;
    info?: string;
  };
}

export interface Community {
  id: number;
  name: string;
  description: string | null;
  rules: string | null;
  is_private: boolean;
  user_id: number;
  created_at: string;
  updated_at: string;
}

export interface Post {
  id: number;
  title: string;
  content: string;
  user_id: number;
  community_id: number;
  created_at: string;
  updated_at: string;
  user?: User;
  comments_count?: number;
  votes?: Vote[];
}

export interface Vote {
  id: number;
  user_id: number;
  post_id: number;
  is_upvote: boolean;
  created_at: string;
  updated_at: string;
}

export interface Comment {
  id: number;
  content: string;
  user_id: number;
  post_id: number;
  parent_id: number | null;
  created_at: string;
  updated_at: string;
  user?: User;
}
