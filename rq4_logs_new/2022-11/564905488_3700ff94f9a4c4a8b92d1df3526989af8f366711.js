import { readable, writable } from "svelte/store";
import file from "../assets/coyotes.wav";

const context = new AudioContext;

export const ctx = readable(context),
  rawData = writable([]);

export const startFromFile = async () => {
  const res = await fetch(file),
    byteArray = await res.arrayBuffer(),
    audioBuffer = await context.decodeAudioData(byteArray),
    source = new AudioBufferSourceNode(context, { buffer: audioBuffer }),
    analyzer = new AnalyserNode(context, { fftSize: 512 });

  source.connect(analyzer);
  analyzer.connect(context.destination);
  source.start();

  const bufferLength = analyzer.frequencyBinCount,
    dataArray = new Uint8Array(bufferLength);

  const update = () => {
    analyzer.getByteFrequencyData(dataArray)
    const orig = Array.from(dataArray);
    rawData.set([[...orig].reverse(), orig].flat());
    requestAnimationFrame(update);
  }

  requestAnimationFrame(update);
}
