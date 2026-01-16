import { createClient } from "microcms-js-sdk";

export type Blog = {
  id: string;
  title: string;
  excerpt: string;
  content: string;
  category: string[];
  tags: string;
  image: {
    url: string;
    height: number;
    width: number;
  };
  readtime: number;
  createdAt: string;
  updatedAt: string;
  publishedAt: string;
  revisedAt: string;
};

if (!process.env.NEXT_PUBLIC_SERVICE_DOMAIN) {
  throw new Error("MICROCMS_SERVICE_DOMAIN is required");
}

if (!process.env.NEXT_PUBLIC_API_KEY) {
  throw new Error("MICROCMS_SERVICE_DOMAIN is required");
}

export const client = createClient({
  serviceDomain: process.env.NEXT_PUBLIC_SERVICE_DOMAIN!,
  apiKey: process.env.NEXT_PUBLIC_API_KEY!,
});

// ブログ一覧を取得
export const getBlogs = async () => {
  const blogs = await client.getList<Blog>({
    endpoint: "blog",
  });
  return blogs;
};

// ブログの詳細を取得
export const getDetail = async (contentId: string) => {
  const blog = await client.getListDetail<Blog>({
    endpoint: "blog",
    contentId,
  });
  return blog;
};