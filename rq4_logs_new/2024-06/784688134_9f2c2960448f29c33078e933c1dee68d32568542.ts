import { ApolloQueryResult, DocumentNode, gql } from '@apollo/client';
import { Description, GraphQlItem } from '@/types/graphql';
import { getGraphQLClient } from '@/api/graphql/api';
import { cache } from 'react';
import ISO6391 from 'iso-639-1';
import { extractAuthors } from '@/lib/data/utils';
import { Item } from '@/types/item';

const graphQLItemQuery: DocumentNode = gql`
  query Item($id: String!) {
    item(id: $id) {
      id
      name
      thumbnail_full_size
      description {
        language
        content
      }
    }
  }
`;

async function fetchGraphQLItem({
  id,
}: {
  id: string;
}): Promise<ApolloQueryResult<{ item: GraphQlItem }>> {
  return getGraphQLClient().query({
    query: graphQLItemQuery,
    variables: { id },
  });
}

export const getGraphQLItem = cache(
  async ({ id }: { id: string }): Promise<Item> => {
    return fetchGraphQLItem({ id }).then((res) => {
      const item = res.data.item;

      return {
        id: item.id,
        name: item.name,
        thumbnail_full_size: item.thumbnail_full_size,
        descriptions: item.description
          .filter(
            (description: Description) =>
              description.language.toLowerCase() !== 'default',
          )
          .map((description: Description) => {
            return {
              language: {
                iso: description.language.toLowerCase(),
                name: ISO6391.getNativeName(description.language.toLowerCase()),
              },
              content: description.content,
            };
          }),
        authors: extractAuthors({ item }),
      };
    });
  },
);