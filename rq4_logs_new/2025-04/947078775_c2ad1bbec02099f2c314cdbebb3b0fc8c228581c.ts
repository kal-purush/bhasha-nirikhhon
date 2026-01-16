import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import {
  deleteLikeSong,
  deleteLikeSongArray,
  getLikeSongs,
  postLikeSong,
} from '@/lib/api/likeSong';
import { PersonalSong } from '@/types/song';

// 🎵 좋아요 한 곡 리스트 가져오기
export function useLikeSongQuery() {
  return useQuery({
    queryKey: ['likeSongs'],
    queryFn: getLikeSongs,
  });
}

// 🎵 곡 좋아요 추가
export function usePostLikedSongMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (songId: string) => postLikeSong({ songId }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['likeSongs'] });
    },
  });
}

// 🎵 곡 좋아요 취소
export function useDeleteLikedSongMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (songId: string) => deleteLikeSong({ songId }),
    onMutate: async (songId: string) => {
      queryClient.cancelQueries({ queryKey: ['likeSongs'] });
      const prev = queryClient.getQueryData(['likeSongs']);
      queryClient.setQueryData(['likeSongs'], (old: PersonalSong[]) =>
        old.filter(song => song.song_id !== songId),
      );
      return { prev };
    },
    onError: (error, songId, context) => {
      queryClient.setQueryData(['likeSongs'], context?.prev);
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['likeSongs'] });
    },
  });
}

// 🎵 여러 곡 좋아요 취소
export function useDeleteLikeSongsArrayMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (songIds: string[]) => deleteLikeSongArray({ songIds }),
    onMutate: async (songIds: string[]) => {
      queryClient.cancelQueries({ queryKey: ['likeSongs'] });
      const prev = queryClient.getQueryData(['likeSongs']);
      queryClient.setQueryData(['likeSongs'], (old: PersonalSong[]) =>
        old.filter(song => !songIds.includes(song.song_id)),
      );
      return { prev };
    },
    onError: (error, songIds, context) => {
      queryClient.setQueryData(['likeSongs'], context?.prev);
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['likeSongs'] });
    },
  });
}