import { useMutation, useQueryClient } from '@tanstack/react-query';

import { deleteHistory } from '@/app/actions/delete-history';
import { useToast } from '@/hooks/use-toast/use-toast';

type UseDeleteHistoryProps = {
	sessionId: string;
	onError?: () => void;
};

export function useDeleteHistory({ sessionId, onError }: UseDeleteHistoryProps) {
	const queryClient = useQueryClient();
	const { toast } = useToast();

	return useMutation({
		mutationFn: () => deleteHistory({ sessionId }),
		onSuccess: () => {
			queryClient.setQueryData(['initial-messages'], []);
		},
		onError: async (error: Error) => {
			await queryClient.invalidateQueries({ queryKey: ['initial-messages'], exact: true });
			onError?.();
			toast({
				title: 'Oeps!',
				variant: 'destructive',
				description: error.message || 'Er ging iets mis',
			});
		},
	});
}