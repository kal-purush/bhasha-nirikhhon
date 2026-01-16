import fs from 'fs/promises'
import path from 'path'
import matter from 'front-matter'
import { Post } from '@/types/blog'
import { markdownToHtml } from './markdown'

const postsDirectory = path.join(process.cwd(), 'content/posts')

type PostMetadata = {
  readonly title: string;
  readonly date: string;
  readonly category?: string;
  readonly tags?: string[];
}

export const listPostFilenames = async (): Promise<Array<string>> => {
  return await fs.readdir(postsDirectory);
};

export const getPostByFilename = async (filename: string): Promise<Post> => {
  const slug = filename.replace(/\.md$/, "");
  return await getPostBySlug(slug);
};

export const getPostBySlug = async (slug: string): Promise<Post> => {
  const fullPath = path.join(postsDirectory, `${slug}.md`);
  const raw = await fs.readFile(fullPath, "utf8");
  const { attributes, body } = matter<PostMetadata>(raw);
  const parsedBody = await markdownToHtml(body);

  return{
    slug: slug,
    title: attributes.title,
    raw: body,
    html: parsedBody,
    date: new Date(attributes.date),
    category: attributes.category ?? "",
    tags: attributes.tags ?? [],
  };
};

export const getLastPosts = async (n: number = 5): Promise<Array<Post>> => {
  const allPosts = await getAllPosts();
  return allPosts.slice(0, n);
};

export const getAllPosts = async (): Promise<Array<Post>> => {
  const filenames = await listPostFilenames();
  const listingPosts = filenames.
    filter((filename) => filename != "preview.md").
    map((filename) => getPostByFilename(filename));
  const posts = await Promise.all(listingPosts);

  return posts.sort((a, b) => b.date.getTime() - a.date.getTime());
};