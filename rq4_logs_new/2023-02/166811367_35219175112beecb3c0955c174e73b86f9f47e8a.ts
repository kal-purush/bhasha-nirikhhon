import { useState, useEffect, useCallback } from 'react';

export default function useSoundPreview() {
  const [previewVolume, setPreviewVolume] = useState('.5');
  const [previewGain, setPreviewGain] = useState<GainNode | null>(null);
  useEffect(() => {
    if (previewGain)
      previewGain.gain.value = Number(previewVolume);
  }, [previewVolume, previewGain]);

  const previewRequest = useCallback(async (soundId: string) => {
    const soundUrl = await fetch(`/api/preview/${ soundId }`, { headers: { 'Content-Type': 'text/plain' } });
    if (soundUrl.status === 401) {
      window.location.reload();
      return;
    }
    const audioRes = await fetch(await soundUrl.text());
    const resBuffer = await audioRes.arrayBuffer();
    const context = new AudioContext();
    const gain = context.createGain();

    setPreviewGain(gain);

    await context.decodeAudioData(resBuffer, buffer => {
      const source = context.createBufferSource();
      source.buffer = buffer;
      source.connect(gain).connect(context.destination);
      source.start(0);
    });
  }, [previewVolume]);

  return { previewRequest, setPreviewVolume };
}