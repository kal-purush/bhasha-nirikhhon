import { gql, useMutation } from "@apollo/client"

interface DeleteReviewVariables {
  deleteReviewId: string
}

const DELETE_REVIEW = gql`
  mutation (
    $deleteReviewId: ID!
  ) {
    deleteReview(
      id: $deleteReviewId
    )
  }
`

export const useDeleteReviewMutation = () =>
  useMutation<unknown, DeleteReviewVariables>(DELETE_REVIEW)