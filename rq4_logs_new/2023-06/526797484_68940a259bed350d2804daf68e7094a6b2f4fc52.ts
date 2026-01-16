import { Client } from '@notionhq/client';
import { QueryDatabaseResponse } from '@notionhq/client/build/src/api-endpoints';

const notion = new Client({ auth: process.env.NOTION_API_KEY });

type BlogPost = {
  id: string;
  title: string;
  createDate: string;
  updateDate: string;
  tags: string[];
  status: string;
  article_id: string;
}[];

export const getArticleMetadata = async (): Promise<BlogPost> => {
  const response: QueryDatabaseResponse = await notion.databases.query({
    database_id: process.env.NOTION_DATABASE_ID as string,
  });

  // [DEBUG]
  // console.log(`response: ${JSON.stringify(response)}`)

  const responseWithProperties = await Promise.all(
    response.results.map(async (post) => {
      if (!('properties' in post)) return null;

      const title = (
        post.properties.title as { rich_text: { plain_text: string }[] }
      ).rich_text[0].plain_text;
      const createDate = (
        post.properties.create_date as { date: { start: string } }
      ).date.start;
      const tags = (
        post.properties.tags as { multi_select: { name: string }[] }
      ).multi_select.map((tag) => tag.name);
      const status = (post.properties.status as { status: { name: string } })
        .status.name;
      const article_id = (
        post.properties.article_id as { title: { plain_text: string }[] }
      ).title[0].plain_text;

      const postInfo = {
        id: post.id || '',
        title: title || '',
        createDate: createDate || '',
        updateDate: post.last_edited_time || '',
        tags: tags || [],
        status: status || '',
        article_id: article_id || '',
      };

      return postInfo;
    }),
  );

  return responseWithProperties.filter((post) => post !== null) as BlogPost;
};