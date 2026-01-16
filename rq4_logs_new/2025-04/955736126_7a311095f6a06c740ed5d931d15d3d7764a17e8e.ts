import { gql } from "@apollo/client";

export const GET_PRODUCT_DETAIL = gql`
  query GetProductDetail($id: String!) {
    product(id: $id) {
      id
      name
      in_stock
      gallery
      description
      brand {
        name
      }
      attributes {
        id
        name
        type {
          name
        }
        items {
          id
          value
          display_value
        }
      }
      prices {
        amount
        currency {
          label
          symbol
        }
      }
    }
  }
`;