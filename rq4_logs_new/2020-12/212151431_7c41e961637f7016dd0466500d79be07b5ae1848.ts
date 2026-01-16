import { PriceTier } from '../../../../@types/__generated__/globalTypes';
import { HarborTiersProps } from '../HarborTiers';

export const data: HarborTiersProps['data'] = {
  __typename: 'HarborNodeConnection',
  edges: [
    {
      __typename: 'HarborNodeEdge',
      node: {
        __typename: 'HarborNode',
        id: '98b7ab56-9c67-46dd-81b2-9eb6f66cbabe',
        properties: {
          __typename: 'HarborProperties',
          name: 'Pound Sterling',
          piers: {
            __typename: 'PierNodeConnection',
            edges: [
              {
                __typename: 'PierNodeEdge',
                node: {
                  __typename: 'PierNode',
                  id: '2030fa0e-c125-44dd-b44e-2b9be403cff6',
                  properties: {
                    __typename: 'PierProperties',
                    priceTier: PriceTier.TIER_1,
                  },
                },
              },
            ],
          },
        },
      },
    },
  ],
};