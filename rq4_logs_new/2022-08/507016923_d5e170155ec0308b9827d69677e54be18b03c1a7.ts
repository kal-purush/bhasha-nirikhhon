export default class NftModelsForUrlsReq {
    denomIds: string[];

    constructor(denomIds: string[]) {
        this.denomIds = denomIds;
    }

    buildRequest() {

        const data = {
            operationName: 'GetNftModelsForUrls',
            query: `query GetNftModelsForUrls {
                nft_nft(where: {
                    denom_id: {_in: [${this.denomIds.map((denom) => `"${denom}"`).join(',')}]},
                    burned: {_eq: false}
                  },
                  distinct_on: denom_id
                ) {
                    uri
                    denom_id
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