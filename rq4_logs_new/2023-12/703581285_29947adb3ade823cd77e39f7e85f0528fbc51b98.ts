import { Category } from './types/index'

type Props = {
  category: Category
  id: string
}

export const addCategory = async (props: Props): Promise<Category> => {
  const { category, id } = props
  const newCategory = {
    ...category,
    uid: id,
  }
  const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/v1/categories`, {
    method: 'POST',
    body: JSON.stringify(newCategory),
    headers: {
      'Content-Type': 'application/json',
    },
  })
  const data = await response.json()
  return data
}