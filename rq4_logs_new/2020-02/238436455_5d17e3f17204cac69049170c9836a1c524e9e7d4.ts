import React, { useState, useEffect } from 'react';

const useAudio = (url: string, isPlay: boolean = false) => {
  const [audio] = useState(new Audio(url));
  const [playing, setPlaying] = useState(isPlay);

  const toggle = () => setPlaying(true);

  useEffect(() => {
    audio.loop = false;
    audio.autoplay = true;
  });

  useEffect(() => {
    playing ? audio.play() : audio.pause();
    return () => {
      audio.pause();
    }
  }, [playing]);

  useEffect(() => {
    audio.addEventListener('ended', () => setPlaying(false));
    return () => {
      audio.removeEventListener('ended', () => setPlaying(false));
    };
  }, []);

  return [playing, toggle];
};

export default useAudio;