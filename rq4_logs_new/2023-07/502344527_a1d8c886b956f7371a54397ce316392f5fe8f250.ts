import { BlogQuery } from "../api/query";
import { Optional } from "../types";

type APIArticle = BlogQuery["blogModel"];
export type Category = {
  name: string;
};
export type Description = {
  text: Optional<string>;
  title: Optional<string>;
  twitterCard: string;
};
export type Article = {
  id: string;
  title: Optional<string>;
  content: string;
  publishedAt: string;
};

type GetArticle = (article: APIArticle) => Article;
export const getArticle: GetArticle = (article) => {
  return {
    id: article.id,
    title: article.title,
    content: article.content,
    publishedAt: article._publishedAt,
  };
};