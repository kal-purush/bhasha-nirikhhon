export interface Node<T> {
  node: T;
}

export interface Post {
  id: string;
  html: string;
  fields: {
    slug: string;
  };
  frontmatter: {
    title: string;
    description: string;
    date: string;
  };
  timeToRead: number;
}

export interface GraphQLResponse<T> {
  data: T;
  pageContext?: {
    current: number;
    skip: number;
    limit: number;
    total: number;
  };
}

export interface AllMarkdownQuery<T> {
  allMarkdownRemark: {
    edges: Node<T>[];
  };
}

export interface MarkdownQuery<T> {
  markdownRemark: T;
}

export interface SiteQuery {
  site: {
    siteMetadata: {
      title: string;
      description: string;
    };
  };
}