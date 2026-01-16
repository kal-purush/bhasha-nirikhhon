// A TypeScript port of https://github.com/Jam3/ios-safe-audio-context
// Which fixes an iOS bug where the sample rate of Web Audio is sect incorrectly

declare const webkitAudioContext: any;
const STANDARD_SAMPLE_RATE = 44100;

export default function createAudioContext(): AudioContext {
  const CompatibleAudioContext = typeof AudioContext !== 'undefined' ? AudioContext : webkitAudioContext;
  let context = new CompatibleAudioContext() as AudioContext;

  // Check if hack is necessary. Only occurs in iOS6+ devices
  // and only when you first boot the iPhone, or play a audio/video
  // with a different sample rate
  if (/(iPhone|iPad)/i.test(navigator.userAgent) &&
      context.sampleRate !== STANDARD_SAMPLE_RATE) {
    const buffer = context.createBuffer(1, 1, STANDARD_SAMPLE_RATE)
    const dummy = context.createBufferSource()
    dummy.buffer = buffer
    dummy.connect(context.destination)
    dummy.start(0)
    dummy.disconnect()
    
    context.close() // dispose old context
    context = new CompatibleAudioContext();
  }

  return context
}