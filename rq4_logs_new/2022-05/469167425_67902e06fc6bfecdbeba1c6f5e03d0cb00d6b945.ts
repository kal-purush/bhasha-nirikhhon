import { gql } from "@apollo/client";

export const CREATE_RESTAURANT_MUTATION = gql`
  mutation CreateRestaurantMutation($data: RestaurantCreateInput!) {
    createRestaurant(data: $data) {
      id
      name
    }
  }
`;