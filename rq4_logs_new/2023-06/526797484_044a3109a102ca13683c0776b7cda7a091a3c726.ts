import { readFileSync } from 'fs';
import matter from 'gray-matter';
import path from 'path';
import { remark } from 'remark';
import html from 'remark-html';

export const getMarkdown = async (id: string) => {
  const filePath = path.join(process.cwd(), 'src/contents', `${id}.md`);
  const fileContents = readFileSync(filePath, 'utf8');
  return fileContents;
};

export const markdownToHtml = async (markdownContent: string) => {
  const matterData = matter(markdownContent);
  const result = await remark().use(html).process(matterData.content);
  return result.toString();
};