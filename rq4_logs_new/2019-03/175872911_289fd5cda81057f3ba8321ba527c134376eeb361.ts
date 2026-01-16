import gql from "graphql-tag";

export const productSchema = gql`
    type Query {
        searchProducts: [ProductHit]
    }
    type ProductHit {
        brand: String
        categories: [String]
        description: String
        free_shipping: Boolean
        image: String
        name: String
        objectID: String
        popularity: Int
        price: Float
        price_range: String
        rating: Int
        type: String
        url: String
    }

    type ProductSearch {
        query: String
        params: String
        exhaustiveFacetsCount: Boolean
        exhaustiveNbHits: Boolean
        page: Int
        nbPages: Int
        hitsPerPage: Int
        processingTimeMS: Int
        hits: [ProductHit]
        nbHits: Int
    }
`;