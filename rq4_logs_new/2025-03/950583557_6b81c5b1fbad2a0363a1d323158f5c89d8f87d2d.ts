import type { WakuPhoneMessage } from "./waku";
import { MediaStreams } from "./media";

export enum CallState {
  IDLE = "IDLE",
  CALLING = "CALLING",
  RINGING = "RINGING",
  IN_CALL = "IN_CALL",
  ENDED = "ENDED",
}

export enum Role {
  Caller = "Caller",
  Called = "Called"
}

type HandleIceCandidatesCallback = (event: RTCPeerConnectionIceEvent) => void;

type CallParams = {
  role: Role,
  callId: string;
  callerId: string;
  calledId: string;
  events: EventTarget;
  localAudio: HTMLAudioElement;
  remoteAudio: HTMLAudioElement;
  handleIceCandidates: HandleIceCandidatesCallback,
};

const DEFAULT_STUN = "stun:stun.l.google.com:19302";

export class Call {
  private readonly role: Role;
  private readonly calledId: string;
  private readonly callerId: string;
  private readonly events: EventTarget;
  private readonly mediaStreams: MediaStreams;
  private readonly handleIceCandidates: HandleIceCandidatesCallback;

  private rtcConnection: RTCPeerConnection;

  constructor(params: CallParams) {
    this.role = params.role;
    this.calledId = params.calledId;
    this.callerId = params.callerId;
    this.events = params.events;
    this.handleIceCandidates = params.handleIceCandidates;

    this.rtcConnection = new RTCPeerConnection({
      iceServers: [{ urls: DEFAULT_STUN }],
    });

    this.mediaStreams = new MediaStreams(params.localAudio, params.remoteAudio, this.rtcConnection);

    this.onStateChange = this.onStateChange.bind(this);

    this.rtcConnection.addEventListener("connectionstatechange", this.onStateChange);
    this.rtcConnection.addEventListener("icecandidate", this.handleIceCandidates);

    this.mediaStreams.setupLocalStream();
    this.mediaStreams.setupRemoteStream();
  }

  public isForWakuMessage(message: WakuPhoneMessage): boolean {
    return message?.calledPeerId === this.calledId && message.callerPeerId === this.callerId;
  }

  public async stop(): Promise<void> {
    if (!this.rtcConnection) {
      return;
    }

    this.rtcConnection.removeEventListener("icecandidate", this.handleIceCandidates);
    this.rtcConnection.close();

    this.mediaStreams.stopStreams();
  }

  public async prepareOffer(): Promise<RTCSessionDescriptionInit> {
    const offer = await this.rtcConnection.createOffer();
    await this.rtcConnection.setLocalDescription(offer);

    this.events.dispatchEvent(new CustomEvent(
      "call:state",
      { detail: CallState.RINGING }
    ));

    return offer;
  }

  public async prepareAnswer(offer: object): Promise<RTCSessionDescriptionInit> {
    await this.rtcConnection.setRemoteDescription(
      new RTCSessionDescription(offer as RTCSessionDescriptionInit)
    );

    const answer = await this.rtcConnection.createAnswer();
    this.rtcConnection.setLocalDescription(answer);

    return answer;
  }

  public async acceptCandidate(candidate: object): Promise<void> {
    await this.rtcConnection.addIceCandidate(
      new RTCIceCandidate(candidate)
    );
  }

  public async acceptAnswer(answer: object): Promise<void> {
    await this.rtcConnection.setRemoteDescription(
      new RTCSessionDescription(answer as RTCSessionDescriptionInit)
    );
  }

  private onStateChange(_event: Event): void {
    const endState = ["closed", "failed", "disconnected"].includes(this.rtcConnection.connectionState);
    if (endState) {
      this.events.dispatchEvent(new CustomEvent(
        "call:state",
        { detail: CallState.ENDED }
      ));
      return;
    }

    if (this.rtcConnection.connectionState === "connected") {
      this.events.dispatchEvent(new CustomEvent(
        "call:state",
        { detail: CallState.IN_CALL }
      ));
      return;
    }
  }
}