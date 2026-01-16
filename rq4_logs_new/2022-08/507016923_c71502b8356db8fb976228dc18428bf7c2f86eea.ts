export default class TotalNumberOfNftsInColletionReq {
    denomId: string;

    constructor(denomId: string) {
        this.denomId = denomId;
    }

    buildRequest() {
        const data = {
            operationName: 'GetTotalNumberOfNftsInCollection',
            query: `query GetTotalNumberOfNftsInCollection {
              nft_nft_aggregate(where: 
                {
                  burned: {_eq: false}, 
                  denom_id: {_eq: "${this.denomId}"}
              }) {
                aggregate {
                  count
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