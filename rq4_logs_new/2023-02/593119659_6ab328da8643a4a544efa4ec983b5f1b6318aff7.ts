import { graphql } from "../generated";

export const BlogCategoryFragment = graphql(`
  fragment BlogCategoryFragment on BlogCategory {
    id
    name
  }
`);

export const BlogPostFragment = graphql(`
  fragment BlogPostFragment on BlogPost {
    id
    subject
    category {
      ...BlogCategoryFragment
    }
    thumbnailFile {
      ...SharedFileFragment
    }
  }
`);

export const BlogPostConnectionFragment = graphql(`
  fragment BlogPostConnectionFragment on BlogPostConnection {
    pageInfo {
      ...PageInfoFragment
    }
    edges {
      node {
        ...BlogPostFragment
      }
      cursor
    }
    totalCount
  }
`);