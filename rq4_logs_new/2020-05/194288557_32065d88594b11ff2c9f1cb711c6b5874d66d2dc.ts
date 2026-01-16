import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import fm from 'front-matter';
import MarkdownIt from 'markdown-it';

const md = new MarkdownIt();

interface Blog {
  title: string;
  slug: string;
  date: string;
}

export const blogDir = (): string => path.join(process.cwd(), '_content/blog');

export const blogFileNames = (): Promise<string[]> => promisify(fs.readdir)(blogDir());

export const parseFileName = (filename: string): { date: string; slug: string } => {
  const [year, month, day, ...slugs] = filename.replace(/\.md$/, '').split('-');

  return {
    date: [day, month, year].join('/'),
    slug: [year, month, day, slugs.join('-')].join('/'),
  };
};

export const readBlogFile = async (filename: string): Promise<Blog & { body: string }> => {
  const { date, slug } = parseFileName(filename);
  const filePath = path.join(blogDir(), filename);
  const fileContents = await promisify(fs.readFile)(filePath, 'utf8');
  const { attributes, body } = fm<Blog>(fileContents);
  return {
    ...attributes,
    body: md.render(body),
    date,
    slug,
  };
};