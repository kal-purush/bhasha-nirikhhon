import { getAudioContext } from "../lib/audioContextSingleTone";
import { GetYTSongBufferResponse } from "./getYTSongBuffer";

export const fetchDemoSongs = async (songName:string): Promise<GetYTSongBufferResponse> => {
    const APP_URL = import.meta.env.VITE_APP_URL
    const response = await fetch(`${APP_URL}/${songName}`);
    if (!response.ok) throw new Error(`Failed to fetch ${songName} file`);
    const audioCtx = getAudioContext()
    const blob = await response.blob()
    const arrayBuffer = await blob.arrayBuffer()
    const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer)
    return {
        audioBuffer,
        blob
    }
}