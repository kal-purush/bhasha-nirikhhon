
export class MediaStreams {

    private localStream?: MediaStream;
    private localAudioElement: HTMLAudioElement;
    private remoteAudioElement: HTMLAudioElement;
    private rtcConnection: RTCPeerConnection;

    public constructor(
        localAudioElement: HTMLAudioElement, 
        remoteAudioElement: HTMLAudioElement,
        rtcConnection: RTCPeerConnection) {
        this.rtcConnection = rtcConnection;
        this.localAudioElement = localAudioElement;
        this.remoteAudioElement = remoteAudioElement;   
    }

    public async setupLocalStream(): Promise<void> {
        this.localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        this.localAudioElement.srcObject = this.localStream;
        //@ts-ignore
        this.localStream.getAudioTracks().forEach(track =>  this.rtcConnection.addTrack(track, this.localStream));
    }

    public async setupRemoteStream(): Promise<void> {
        this.rtcConnection.addEventListener("track", e => {
            this.remoteAudioElement.srcObject = e.streams[0];
          });
    }

    public async stopStreams(): Promise<void> {
        this.localStream?.getTracks().forEach(track => track.stop());
    }
}