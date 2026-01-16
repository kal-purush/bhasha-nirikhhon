import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { addToLibrary, addToWishList, type AddGameRequest } from '@/core/api/services/library.service';

export function useAddToLibrary() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (gameData: AddGameRequest) => addToLibrary(gameData),
    onSuccess: (data) => {
      // Invalidate libraries queries to refresh data
      queryClient.invalidateQueries({ queryKey: ['library'] });

      // Show success toast
      toast.success(`${data.game.name} added to your library!`);
    },
    onError: (error) => {
      toast.error('Failed to add game to library');
      console.error('Add to library error: ', error);
    }
  });
}

export function useAddToWishlist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (gameData: AddGameRequest) => addToWishList(gameData),
    onSuccess: (data) => {
      // Invalidate wishlist queries to refresh data
      queryClient.invalidateQueries({ queryKey: ['wishlist'] });

      // Show success toast
      toast.success(`${data.game.name} added to your wishlist!`);
    },
    onError: (error) => {
      toast.error('Failed to add game to wishlist');
      console.error('Add to wishlist error:', error);
    }
  });

}