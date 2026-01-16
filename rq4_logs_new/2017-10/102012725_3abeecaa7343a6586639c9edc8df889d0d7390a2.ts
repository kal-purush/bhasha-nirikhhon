import { Kali } from 'kali';
import { TimeStretcher } from './time-stretcher';

class Test extends TimeStretcher {
  timeStretch(
    sourceBuffer: AudioBuffer,
    goalBuffer: AudioBuffer,
    ratio: number
  ): void {
    const numInputFrames = sourceBuffer.length;
    const bufsize = 4096;
    // based on the example from the kali docs
    for (let c = 0; c < sourceBuffer.numberOfChannels; ++c) {
      let inputOffset: number = 0;
      let completedOffset: number = 0;
      let flushed = false;
      const kali = new Kali(1);
      kali.setup(sourceBuffer.sampleRate, ratio);
      const inputData = sourceBuffer.getChannelData(c);
      const completed = goalBuffer.getChannelData(c);
  
      while (completedOffset < completed.length) {
        // Read stretched samples into our output array
        completedOffset += kali.output(
          completed.subarray(
            completedOffset,
            Math.min(completedOffset + bufsize, completed.length)
          ));
        
        if (inputOffset < inputData.length) { // If we have more data to write, write it
          const dataToInput: Float32Array = inputData.subarray(
            inputOffset,
            Math.min(inputOffset + bufsize, inputData.length)
          );
          inputOffset += dataToInput.length;
          
          // Feed Kali samples
          kali.input(dataToInput);
          kali.process();
        } else if (!flushed) { // Flush if we haven't already
          kali.flush();
          flushed = true;
        }
      }
    }
  }
}