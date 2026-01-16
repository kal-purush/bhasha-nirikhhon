import { uri } from "@youmeet/functions/imports";
import {
  getArticlesParams,
  getCompetenciesParams,
  getOffersParams,
  getUsersParams,
} from "@youmeet/functions/request";
import { Article, BetaUser, Competency, Offer } from "@youmeet/gql/generated";

export default async function sitemap(): Promise<any> {
  const offers = (await getOffersParams<Offer[]>()) as Offer[];
  const users = (await getUsersParams<BetaUser[]>({
    data: {
      user: true,
      isScrapped: false,
      isPublic: true,
      isVideo: true,
    },
  })) as BetaUser[];
  const competencies = (await getCompetenciesParams<
    Competency[]
  >()) as Competency[];
  const articles = (await getArticlesParams<Article[]>()) as Article[];

  return [
    {
      url: `${uri}`,
      lastModified: new Date(),
      changeFrequency: "monthly",
      priority: 1,
    },
    {
      url: `${uri}/se-connecter`,
      lastModified: new Date(),
      changeFrequency: "monthly",
      priority: 0.5,
    },
    {
      url: `${uri}/offres`,
      lastModified: new Date(),
      changeFrequency: "daily",
      priority: 1,
    },
    {
      url: `${uri}/blog`,
      lastModified: new Date(),
      changeFrequency: "daily",
      priority: 1,
    },
    {
      url: `${uri}/le-produit/mise-en-relation`,
      lastModified: new Date(),
      changeFrequency: "weekly",
      priority: 0.8,
    },
  ].concat(
    offers.map((offer) => ({
      url: `${uri}/offres/${offer.slug}`,
      lastModified: new Date(),
      changeFrequency: "always",
      priority: 1,
    })) as any,
    users.map((user) => ({
      url: `${uri}/on/${user.uniqueName}`,
      lastModified: new Date(),
      changeFrequency: "always",
      priority: 1,
    })) as any,
    articles.map((article) => ({
      url: `${uri}/medias/${article.slug}`,
      lastModified: new Date(),
      changeFrequency: "always",
      priority: 1,
    })) as any,
    competencies.map((competency) => ({
      url: `${uri}/competences/${competency.slug}`,
      lastModified: new Date(),
      changeFrequency: "monthly",
      priority: 0.5,
    })) as any
  );
}