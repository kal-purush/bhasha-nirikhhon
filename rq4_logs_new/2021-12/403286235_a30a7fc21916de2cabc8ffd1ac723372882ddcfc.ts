import {
  CategoriesQuery,
  CategoriesQueryVariables,
  useCategoriesQuery,
} from "src/graphql/types"
import { connectionToNodes } from "src/ui/common/utils/connectionToNodes"

export function useCategories(userId: string) {
  const { data, fetchMore, loading, error } = useCategoriesQuery({
    variables: { userId: userId },
  })

  const pageInfo = data?.categories.pageInfo
  const categories = connectionToNodes(data?.categories)

  async function fetchMoreCategories() {
    try {
      await fetchMore<CategoriesQuery, CategoriesQueryVariables>({
        variables: {
          cursor: pageInfo?.endCursor ?? "",
        },
      })
    } catch (e) {
      console.error(e)
    }
  }

  return {
    loading: loading,
    error: error,
    categories: categories,
    pageInfo: pageInfo,
    fetchMore: fetchMoreCategories,
  }
}