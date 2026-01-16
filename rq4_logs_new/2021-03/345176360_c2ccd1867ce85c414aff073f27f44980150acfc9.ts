import { gql } from '@apollo/client'

export const CREATE_ITEM = gql`
  mutation CREATE_ITEM(
    $listId: ID!
    $category: ID!
    $width: Float
    $height: Float
    $depth: Float
    $weight: Int
    $URL: String
    $quantity: Int
    $name: String
    $description: String
  ) {
    itemCreate(
      data: {
        width: $width
        height: $height
        depth: $depth
        weight: $weight
        uRL: $URL
        quantity: $quantity
        name: $name
        description: $description
        packingList: { connect: { id: $listId } }
        category: { connect: { id: $category } }
      }
    ) {
      id
    }
  }
`