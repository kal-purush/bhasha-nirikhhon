import { gql } from '@apollo/client'

export const UPDATE_ITEM = gql`
  mutation UPDATE_ITEM(
    $id: ID!
    $listId: ID!
    $category: ID!
    $width: Float
    $height: Float
    $depth: Float
    $weight: Int
    $URL: String
    $price: Float
    $quantity: Int
    $name: String
    $description: String
    $color: String
  ) {
    itemUpdate(
      data: {
        id: $id
        width: $width
        height: $height
        depth: $depth
        weight: $weight
        uRL: $URL
        quantity: $quantity
        price: $price
        name: $name
        description: $description
        color: $color
        packingList: { connect: { id: $listId } }
        category: { connect: { id: $category } }
      }
    ) {
      id
    }
  }
`