/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: ListDraftsFragment
// ====================================================

export interface ListDraftsFragment_drafts_pageInfo {
  readonly __typename: "OffsetBasePageInfo";
  readonly hasNextPage: boolean;
  readonly nextPage: number | null;
}

export interface ListDraftsFragment_drafts_nodes {
  readonly __typename: "Draft";
  readonly id: string;
  readonly title: string | null;
  readonly createdAt: any;
}

export interface ListDraftsFragment_drafts {
  readonly __typename: "DraftConnection";
  readonly pageInfo: ListDraftsFragment_drafts_pageInfo;
  readonly totalCount: number;
  readonly nodes: ReadonlyArray<ListDraftsFragment_drafts_nodes>;
}

export interface ListDraftsFragment {
  readonly __typename: "Diary";
  readonly drafts: ListDraftsFragment_drafts;
}