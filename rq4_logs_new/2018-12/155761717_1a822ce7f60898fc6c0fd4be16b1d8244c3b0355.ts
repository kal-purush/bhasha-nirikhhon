import gql from 'graphql-tag';

export default gql`
extend type Query {
  attractionsPage(page: Int!, city: String!): AttractionsPage!
  attraction(id: ID!): Attraction
}

type AttractionsPage {
  attractions: [Attraction!]!
  hasMore: Boolean!
}

type Attraction {
  id: ID!
  classifications: [Classification!]!
  externalLinks: ExternalLinks
  images: [Image!]!
  name: String!
  upcomingEvents: Int!
  url: String!
}

type ExternalLink {
  platform: String!
  url: String!
}

type ExternalLinks {
  facebook: ExternalLink
  lastfm: ExternalLink
  homepage: ExternalLink
  instagram: ExternalLink
  itunes: ExternalLink
  twitter: ExternalLink
  youtube: ExternalLink
  wiki: ExternalLink
}
`;