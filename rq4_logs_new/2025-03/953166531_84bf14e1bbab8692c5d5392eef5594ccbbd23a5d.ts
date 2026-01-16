export interface Post {
    title: string;
    datetime: Date | undefined;
    caption?: string;
    image?: string;
    video?: string;
    description?: string;
  }