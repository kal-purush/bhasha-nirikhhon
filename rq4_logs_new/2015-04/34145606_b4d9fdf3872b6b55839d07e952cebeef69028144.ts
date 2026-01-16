 module NES.TS {
     export class AudioPlayer {
         audioContext: AudioContext;

         audioNode: AudioBufferSourceNode;

         constructor() {
             this.audioContext = new AudioContext();
         }

         playRandomLoop() {
             // create a BufferSource and a Buffer and then get the channel data of the created buffer
             // NOTE: The BufferSource is actually a BufferSourceNode, it can be used only once. When it finished playing or when you stopped it you have to create a new one. 
             this.audioNode = this.audioContext.createBufferSource();

             var buffer = this.audioContext.createBuffer(1, 4096, this.audioContext.sampleRate);
             var data = buffer.getChannelData(0);

             // fill the data with random noises
             for (var i = 0; i < 4096; i++) {
                 data[i] = Math.random();
             }

             // set the buffer on the node and connect it to the audio context
             this.audioNode.buffer = buffer;
             this.audioNode.loop = true;
             this.audioNode.connect(this.audioContext.destination);
             this.audioNode.start(0);
         }

         playInts(values: number[]) {
             this.audioNode = this.audioContext.createBufferSource();

             var buffer = this.audioContext.createBuffer(2, values.length, this.audioContext.sampleRate);
             var leftData = buffer.getChannelData(0);
             var rightData = buffer.getChannelData(1);

             // fill the data
             var j = 0;
             for (var i = 0; i < values.length; i+=2) {
                 leftData[j] = values[i] / 32768;
                 rightData[j] = values[i + 1] / 32768;
                 j++;
             }

             // set the buffer on the node and connect it to the audio context
             this.audioNode.buffer = buffer;
             //this.audioNode.loop = true;
             this.audioNode.connect(this.audioContext.destination);
             this.audioNode.start(0);
         }
     }
 }