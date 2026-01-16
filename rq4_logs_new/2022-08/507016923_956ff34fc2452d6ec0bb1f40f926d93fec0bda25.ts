import Config from '../../../../../../../builds/dev-generated/Config';

export default class NftCollectionModelsPaginatedReq {
    owner: string;
    from: number;
    to: number;
    filter: string;

    constructor(owner: string, from: number, to: number, filter: string) {
        this.owner = owner;
        this.from = from;
        this.to = to;
        this.filter = filter;
    }

    buildRequest() {
        const data = {
            operationName: 'GetNftCollectionsOwnerAndFilterPaginated',
            query: `query GetNftCollectionsOwnerAndFilterPaginated {
                nft_nft_aggregate(where: {_or: [
                    {
                      owner: {_eq: "${this.owner}"},
                      denom_id: {_neq: "${Config.CUDOS_NETWORK.NFT_DENOM_ID}"},
                      burned: {_eq: false},
                      nft_denom: {name: {_iregex: "${this.filter}"}}
                    }, 
                    {nft_denom: {
                      owner: {_eq: "${this.owner}"},
                      name: {_iregex: "${this.filter}"}
                    }
                }]}) {
                aggregate {
                  count
                }
                nodes {
                    nft_denom {
                      id
                      name
                      owner
                      schema
                      symbol
                      transaction_hash
                      contract_address_signer
                    }
                  }
              }
            }`,
            variables: null,
        };

        return {
            method: 'POST',
            body: JSON.stringify(data),
        }
    }
}