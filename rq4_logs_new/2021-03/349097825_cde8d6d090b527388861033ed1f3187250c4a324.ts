import React, { Dispatch } from 'react';

export const audioStart = (
  audioRef: React.MutableRefObject<HTMLAudioElement>,
  audioSrc: Array<string>,
  setIsAudioPlay: Dispatch<React.SetStateAction<boolean>>
): void => {
  const src: Array<string> = [...audioSrc];

  if (!src.length) {
    setIsAudioPlay((s) => !s);
    return;
  }

  const audio = src.shift();
  if (audio) {
    // eslint-disable-next-line no-param-reassign
    audioRef.current.src = audio;
    // eslint-disable-next-line no-param-reassign
    audioRef.current.play();
  }

  // eslint-disable-next-line no-param-reassign
  audioRef.current.onended = () => {
    audioStart(audioRef, src, setIsAudioPlay);
  };
};

export const audioStop = (audioRef: React.MutableRefObject<HTMLAudioElement>): void => {
  // eslint-disable-next-line no-param-reassign
  audioRef.current.pause();
};