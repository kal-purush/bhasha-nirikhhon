export class Echo {
  audioContext: AudioContext
  echoDelay: DelayNode
  echoGain: GainNode

  constructor(audioContext) {
    this.audioContext = audioContext
  }

  addEchoValues(distance) {
    this.echoDelay = this.audioContext.createDelay()
    this.echoDelay.delayTime.value = distance
    this.echoGain = this.audioContext.createGain()
    this.echoGain.gain.value = (0.25 * 1 / distance)
  }

}