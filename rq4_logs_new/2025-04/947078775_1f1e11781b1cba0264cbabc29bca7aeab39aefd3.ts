'use client';

import { useEffect, useState } from 'react';

import useLoadingStore from '@/stores/useLoadingStore';
import useSongStore from '@/stores/useSongStore';

export default function useAddSongList() {
  const [deleteLikeSelected, setDeleteLikeSelected] = useState<string[]>([]);
  const { startLoading, stopLoading, initialLoading } = useLoadingStore();

  const { refreshLikedSongs, refreshRecentSongs, deleteLikedSongs } = useSongStore();

  const handleApiCall = async <T>(apiCall: () => Promise<T>, onError?: () => void) => {
    startLoading();
    try {
      const result = await apiCall();
      return result;
    } catch (error) {
      console.error('API 호출 실패:', error);
      if (onError) onError();
      return null;
    } finally {
      stopLoading();
    }
  };

  const getLikedSongs = async () => {
    await handleApiCall(async () => {
      refreshLikedSongs();
    });
  };

  const getRecentSongs = async () => {
    await handleApiCall(async () => {
      refreshRecentSongs();
    });
  };

  const handleToggleSelect = (songId: string) => {
    setDeleteLikeSelected(prev =>
      prev.includes(songId) ? prev.filter(id => id !== songId) : [...prev, songId],
    );
  };

  const handleDelete = async () => {
    await handleApiCall(async () => {
      await deleteLikedSongs(deleteLikeSelected);
      setDeleteLikeSelected([]);
    });
  };

  const totalSelectedCount = deleteLikeSelected.length;

  useEffect(() => {
    getLikedSongs();
    getRecentSongs();
    initialLoading();
  }, []);

  return {
    deleteLikeSelected,
    totalSelectedCount,
    handleToggleSelect,
    handleDelete,
  };
}