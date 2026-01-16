import { useMeQuery } from "../graphql"
import { Review } from "../types"

export const useMyReviews = (): {
  reviews: ReadonlyArray<Review>,
  fetchNext: () => void
} => {
  const { data, loading, fetchMore } = useMeQuery({
    getReviews: true,
    first: 5,
    after: ""
  })
  const reviews = !data?.me
    ? []
    : data.me.reviews.edges.map(edge => edge.node)

  const fetchNext = () => {
    if (!loading && data?.me) {
      const { endCursor, hasNextPage } = data.me.reviews.pageInfo
      if (hasNextPage) {
        fetchMore({
          variables: {
            after: endCursor
          }
        })
      }
    }
  }
  return { reviews, fetchNext }
}