export const getCompetitionSlug = (slug) => '/' + slug.split('/')[1];

export const computeEdges = (edges, slug) =>
  edges
    .filter((edge) => edge.node.frontmatter.slug.includes(slug))
    .sort((edge1, edge2) =>
      edge1.node.frontmatter.title > edge2.node.frontmatter.title ? 1 : -1
    );

export const getTitle = (slug) =>
  slug
    .slice(1)
    .split('-')
    .map((t) => t[0].toUpperCase() + t.substring(1))
    .join(' ');