import axios from "axios";
import decodeJSON from "./utilities/decode-json";
import { getConnection, getRepository } from "typeorm";

const ENTITY_NAME = "Article";
const UNIQUE_KEY = "news_id";

const insertNewArticles = (articles: any) => {
  articles.forEach((article: any) => {
    getConnection()
      .getRepository(ENTITY_NAME)
      .findOne({ news_id: `${article[UNIQUE_KEY]}` })
      .then((hasArticle: any) => {
        if (!hasArticle) {
          insertArticle(article);
        }
      });
  });
};

const insertArticle = (article: any) => {
  getConnection()
    .createQueryBuilder()
    .insert()
    .into(ENTITY_NAME)
    .values(article)
    .execute();
};

export { insertNewArticles };