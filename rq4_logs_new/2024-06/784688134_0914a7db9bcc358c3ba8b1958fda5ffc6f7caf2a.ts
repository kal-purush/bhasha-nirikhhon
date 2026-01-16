import { ApolloQueryResult, DocumentNode, gql } from '@apollo/client';
import { GraphQlContexts, Context } from '@/types/graphql';
import { getGraphQLClient } from '@/api/graphql/api';
import { cache } from 'react';
import { Building } from '@/types/types';

export const LOCATION_INFO = {
  '!PsyURUpKAbSPistHpQ:content.udk-berlin.de': {
    lng: 13.3286,
    lat: 52.5094,
    image: 7,
    maxZoom: 15.25,
  },
  '!RuJBwEwOQcFrQabJnn:content.udk-berlin.de': {
    lng: 13.3299,
    lat: 52.498635,
    image: 3,
    maxZoom: 14.1,
  },
  '!YIwQSiHDpoiNHDMWmC:content.udk-berlin.de': {
    lng: 13.32215,
    lat: 52.51705,
    image: 4,
    maxZoom: 14.7,
  },
  '!XGSFQYZUnFtQNzOBnD:content.udk-berlin.de': {
    lng: 13.327300406397331,
    lat: 52.50967152502669,
    image: 5,
    maxZoom: 13.7,
  },
  '!GFauydmVRlpqvDETXH:content.udk-berlin.de': {
    lng: 13.3588243,
    lat: 52.4908045,
    image: 6,
    maxZoom: 14.66,
  },
  '!eVjUBtkIgDQkQSKVxm:content.udk-berlin.de': {
    lng: 13.329011660461106,
    lat: 52.513815995810795,
    image: 8,
    maxZoom: 15.45,
  },
  '!OkEblSLtaWAObRcCHm:content.udk-berlin.de': {
    lng: 13.332849698118526,
    lat: 52.49982823444154,
    image: 10,
    maxZoom: 15.6,
  },
  '!fPuAzLpetwUYPJZwCF:content.udk-berlin.de': {
    lng: 13.30389,
    lat: 52.52441,
    image: 11,
    maxZoom: 15.89,
  },
  '!HXlOLrPXYbIQdkqryj:content.udk-berlin.de': {
    lng: 13.32345,
    lat: 52.51129,
    image: 9,
    maxZoom: 16.8,
  },
  '!RpTarLRqYYIdDCBLyV:content.udk-berlin.de': {
    lng: 13.3281,
    lat: 52.50893,
    image: 13,
    maxZoom: 15.2,
  },
  '!YOMEVNrNhhIBxSAhNQ:content.udk-berlin.de': {
    lng: 13.374843,
    lat: 52.551709,
    image: 12,
    maxZoom: 16.1,
  },
  '!CmGOTOZlDoWMcJFHkZ:content.udk-berlin.de': {
    lng: 13.321916506276636,
    lat: 52.51264659409021,
    image: 2,
    maxZoom: 15.2,
  },
  '!IKWVNtgTydTHMgpUwQ:content.udk-berlin.de': {
    lng: 13.36031847972046,
    lat: 52.50000813130538,
    image: 1,
    maxZoom: 15.2,
  },
};

const graphQLContextsQuery: DocumentNode = gql`
  query Contexts {
    contexts(template: "location-building") {
      id
      name
      template
      allocation {
        physical {
          lat
          lng
        }
      }
    }
  }
`;

async function fetchGraphQLContexts(): Promise<
  ApolloQueryResult<GraphQlContexts>
> {
  return getGraphQLClient().query({
    query: graphQLContextsQuery,
    variables: {},
  });
}

export const getBuildings: () => Promise<
  GeoJSON.FeatureCollection<GeoJSON.Point, Building>
> = cache(async () => {
  return fetchGraphQLContexts().then((res) => ({
    type: 'FeatureCollection',
    features: res.data.contexts.map(
      (loc: Context, index): GeoJSON.Feature<GeoJSON.Point, Building> => {
        const coords =
          loc.id in LOCATION_INFO
            ? [LOCATION_INFO[loc.id].lng, LOCATION_INFO[loc.id].lat]
            : loc?.allocation?.physical &&
                loc.allocation.physical[0].lat &&
                loc.allocation.physical[0].lng
              ? [
                  parseFloat(loc.allocation.physical[0].lng),
                  parseFloat(loc.allocation.physical[0].lat),
                ]
              : [0, 0];
        return {
          id: loc.id,
          type: 'Feature',
          properties: {
            id: loc.id,
            name: loc.name,
            image:
              loc.id in LOCATION_INFO ? LOCATION_INFO[loc.id].image : index + 1,
            maxZoom:
              loc.id in LOCATION_INFO ? LOCATION_INFO[loc.id].maxZoom : 17,
          },
          geometry: {
            type: 'Point',
            coordinates: coords,
          },
        };
      },
    ),
  }));
});