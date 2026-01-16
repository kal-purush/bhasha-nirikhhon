export type Memory = {
  id: string;
  userId: string;
  coverUrl: string;
  isPublic: string;
  createdAt: string;
  content: string;
};

export type MemoryList = Omit<Memory, 'content'> & { excerpt: string };