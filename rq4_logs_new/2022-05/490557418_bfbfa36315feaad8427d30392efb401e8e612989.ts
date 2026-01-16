import { gql, useMutation } from "@apollo/client"

interface CreateReviewResponse {
  createReview: {
    repositoryId: string
  }
}

interface CreateReviewVariables {
  review: {
    repositoryName: string,
    ownerName: string,
    rating: number,
    text?: string
  }
}

const CREATE_REVIEW = gql`
  mutation (
    $review: CreateReviewInput
  ) {
    createReview(
      review: $review
    ) {
      repositoryId
    }
  }
`

export const useCreateReviewMutation = () =>
  useMutation<CreateReviewResponse, CreateReviewVariables>(CREATE_REVIEW)