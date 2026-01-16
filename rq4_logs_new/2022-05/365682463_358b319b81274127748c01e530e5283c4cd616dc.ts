import { P2P } from "modules/P2P"
import { useEffect } from "react"

// TODO
const useSpeakDetect = (p2p: P2P) => {

  useEffect(() => {
    if (!p2p) return

    const audioTrack = p2p.stream.getAudioTracks()[0]
    const audioContext = new AudioContext()
    const analyzer = audioContext.createAnalyser()
    audioContext.audioWorklet.addModule("").then(() => {
      
    })

  }, [p2p])
}

export default useSpeakDetect