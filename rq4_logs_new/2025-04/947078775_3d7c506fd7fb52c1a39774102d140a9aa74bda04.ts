'use client';

import { useEffect, useState } from 'react';

import { useLoadingStore } from '@/stores/useLoadingStore';
import { AddListModalSong } from '@/types/song';

export function useAddListModal() {
  const [activeTab, setActiveTab] = useState('liked');
  const [likedSongs, setLikedSongs] = useState<AddListModalSong[]>([]);
  const [recentSongs, setRecentSongs] = useState<AddListModalSong[]>([]);
  const [songSelected, setSongSelected] = useState<string[]>([]);
  const { startLoading, stopLoading } = useLoadingStore();

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
      const response = await fetch('/api/songs/like');
      const { data } = await response.json();
      setLikedSongs(data);
    });
  };

  const getRecentSongs = async () => {
    await handleApiCall(async () => {
      const response = await fetch('/api/songs/recent');
      const { data } = await response.json();
      setRecentSongs(data);
    });
  };

  const handleToggleSelect = (songId: string) => {
    setSongSelected(prev =>
      prev.includes(songId) ? prev.filter(id => id !== songId) : [...prev, songId],
    );
  };

  const handleConfirm = async () => {
    // TODO: API 호출 로직 구현
  };

  const totalSelectedCount = songSelected.length;

  useEffect(() => {
    getLikedSongs();
    getRecentSongs();
  }, []);

  return {
    activeTab,
    setActiveTab,
    likedSongs,
    recentSongs,
    songSelected,
    handleToggleSelect,
    handleConfirm,
    totalSelectedCount,
  };
}