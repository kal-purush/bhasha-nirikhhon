import { UpperCategory } from "../../../types/index";
import { Paths, useGetList, useMutationApi } from "../factory";

export function useUpperCategoryMutations() {
  const {
    deleteItem: deleteUpperCategory,
    updateItem: updateUpperCategory,
    createItem: createUpperCategory,
  } = useMutationApi<UpperCategory>({
    baseQuery: Paths.MenuUpperCategories,
  });

  return { deleteUpperCategory, updateUpperCategory, createUpperCategory };
}

export function useGetUpperCategories() {
  return useGetList<UpperCategory>(Paths.MenuUpperCategories);
}