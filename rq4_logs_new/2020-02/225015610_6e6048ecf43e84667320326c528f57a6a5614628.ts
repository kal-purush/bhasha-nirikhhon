/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: ListPublishedArticlesFragment
// ====================================================

export interface ListPublishedArticlesFragment_publishedArticles_pageInfo {
  readonly __typename: "OffsetBasePageInfo";
  readonly hasNextPage: boolean;
  readonly nextPage: number | null;
}

export interface ListPublishedArticlesFragment_publishedArticles_nodes {
  readonly __typename: "PublishedArticle";
  readonly id: string;
  readonly title: string | null;
  readonly createdAt: any;
}

export interface ListPublishedArticlesFragment_publishedArticles {
  readonly __typename: "PublishedArticleConnection";
  readonly pageInfo: ListPublishedArticlesFragment_publishedArticles_pageInfo;
  readonly totalCount: number;
  readonly nodes: ReadonlyArray<ListPublishedArticlesFragment_publishedArticles_nodes>;
}

export interface ListPublishedArticlesFragment {
  readonly __typename: "Diary";
  readonly publishedArticles: ListPublishedArticlesFragment_publishedArticles;
}