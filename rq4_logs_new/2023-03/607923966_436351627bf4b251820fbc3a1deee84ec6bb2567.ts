import { useMutation, useQueryClient } from '@tanstack/react-query'
import type { UpdateTodoSchema, ParamTodoId } from '@server/model'
import { updateTodo } from '@client/api/todos'

type SuccessResponse = Awaited<ReturnType<typeof updateTodo>>
export const useUpdateTodo = () => {
  const queryClient = useQueryClient()
  const updateIsDoneMutation = useMutation(
    async ({ id, ...other }: ParamTodoId & UpdateTodoSchema) =>
      await updateTodo({ id, ...other }),
    {
      onSuccess: async (data: SuccessResponse) => {
        // TODO: allTodoを定数から取得する
        await queryClient.invalidateQueries(['allTodo'])
        return data
      }
    }
  )
  return updateIsDoneMutation
}