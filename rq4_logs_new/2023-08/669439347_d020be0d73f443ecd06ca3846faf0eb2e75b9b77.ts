import { Box } from '../../services/boxes';
import { useMutation, useQueryClient } from '@tanstack/react-query';

type MutationFn<V> = (variables: V) => Promise<void>;

type OnMutateFn<V> = (previousBoxList: Box[] | undefined, variables: V) => Box[];

interface UseBoxMutationProps<V> {
  mutationFn: MutationFn<V>;
  onMutate: OnMutateFn<V>;
}

const useBoxMutation = <V>({ mutationFn, onMutate }: UseBoxMutationProps<V>) => {
  const queryClient = useQueryClient();
  const queryKey = ['box', 'my'];

  return useMutation({
    mutationFn,
    async onMutate(variables) {
      await queryClient.cancelQueries({ queryKey });

      const previousBoxes = queryClient.getQueryData<Box[]>(queryKey);

      queryClient.setQueryData(queryKey, onMutate(previousBoxes, variables));

      return { previousBoxes };
    },
    onError(_, __, context) {
      if (context?.previousBoxes) {
        queryClient.setQueryData(queryKey, context.previousBoxes);
      }
    },
  });
};

export default useBoxMutation;