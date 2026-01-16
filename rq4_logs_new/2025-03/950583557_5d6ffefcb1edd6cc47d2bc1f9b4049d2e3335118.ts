import type { IDecodedMessage, IDecoder, IEncoder, LightNode } from "@waku/sdk";
import { createDecoder, createEncoder, bytesToUtf8, utf8ToBytes } from "@waku/sdk";

type WakuRTCParams = {
  node: LightNode;
  config?: RTCConfiguration;
};

const DEFAULT_STUN = "stun:stun.l.google.com:19302";
const DEFAULT_CONTENT_TOPIC = "/waku-phone/1/sig01/proto";

export class WakuRTC {
  private started = false;
  private inProgress = false;

  private readonly node: LightNode;
  private readonly encoder: IEncoder;
  private readonly decoder: IDecoder<any>;

  private readonly rtcConnection: RTCPeerConnection;
  private iceCandidates: RTCIceCandidate[] = [];

  private inboundChannel: RTCDataChannel | undefined;
  private readonly outboundChannel: RTCDataChannel;

  private filterUnsubscribe: undefined | (() => void);

  private isFree: boolean = true;

  public constructor(params: WakuRTCParams) {
    this.node = params.node;

    this.encoder = createEncoder({
      contentTopic: DEFAULT_CONTENT_TOPIC,
      pubsubTopicShardInfo: {
        clusterId: 42
      }
    });
    this.decoder = createDecoder(DEFAULT_CONTENT_TOPIC, { clusterId: 42 });

    this.rtcConnection = new RTCPeerConnection({
      iceServers: [{ urls: DEFAULT_STUN }],
      ...(params.config || {})
    });
    this.outboundChannel = this.rtcConnection.createDataChannel("outbound");

    this.onICECandidate = this.onICECandidate.bind(this);
    this.onInboundChannel = this.onInboundChannel.bind(this);
  }

  public async start(): Promise<void> {
    if (this.started || this.inProgress) {
      return;
    }

    this.inProgress = true;

    this.rtcConnection.addEventListener("datachannel", this.onInboundChannel);
    this.rtcConnection.addEventListener("icecandidate", this.onICECandidate);

    try {
      this.filterUnsubscribe = await this.node.filter.subscribeWithUnsubscribe(this.decoder, this.onWakuMessage);
    } catch(e) {
      console.error("Error while Filter subscribe:", e);
    }

    this.inProgress = false;
    this.started = true;
  }

  public async stop(): Promise<void> {
    if (!this.started || this.inProgress) {
      return;
    }

    this.inProgress = true;

    this.rtcConnection.removeEventListener("datachannel", this.onInboundChannel);
    this.rtcConnection.removeEventListener("icecandidate", this.onICECandidate);

    try {
      this?.filterUnsubscribe?.();
    } catch(e) {
      console.error("Error while Filter unsubscribe:", e);
    }

    this.inProgress = false;
    this.started = false;
  }

  public async initiateConnection(peerId: string): Promise<void> {
    await this.sendWakuMessage("call", this.node.peerId);
  }

  private onInboundChannel(event: RTCDataChannelEvent): void {
    this.inboundChannel = event.channel;
  }

  private async onICECandidate(event: RTCPeerConnectionIceEvent): Promise<void> {
    if (!event.candidate) {
      return;
    }

    this.iceCandidates.push(event.candidate);
    await this.sendWakuMessage("candidate", this.iceCandidates);
  }

  private async onWakuMessage(message: IDecodedMessage): Promise<void> {
    const payload = bytesToUtf8(message.payload);
    const data = JSON.parse(payload);

    if (data.type === "call") {
      await this.onConnectionRequestMessage(data.payload);
    } else if (data.type === "candidate") {
      await this.onCandidateMessage(data.payload);
    } else if (data.type === "offer") {
      await this.onOfferMessage(data.payload);
    } else if (data.type === "answer") {
      await this.onAnswerMessage(data.payload);
    } else if (data.type === "ready") {
      this.onReadyMessage();
    }
  }

  private async onCandidateMessage(candidates: RTCIceCandidate[]): Promise<void> {
    for (const candidate of candidates) {
      await this.rtcConnection.addIceCandidate(
        new RTCIceCandidate(candidate)
      );
    }
  }

  private async onOfferMessage(offer: RTCSessionDescriptionInit): Promise<void> {
    await this.rtcConnection.setRemoteDescription(
      new RTCSessionDescription(offer)
    );

    const answer = await this.rtcConnection.createAnswer();
    this.rtcConnection.setLocalDescription(answer);

    await this.sendWakuMessage("answer", answer);
  }

  private async onAnswerMessage(answer: RTCSessionDescriptionInit) {
    await this.rtcConnection.setRemoteDescription(
      new RTCSessionDescription(answer)
    );
  }

  private async onConnectionRequestMessage(peerId: string): Promise<void> {
    if(!this.isFree || !this.node.peerId.equals(peerId)) {
      return;
    }

    this.isFree = false;

    const offer = await this.rtcConnection.createOffer();
    await this.rtcConnection.setLocalDescription(offer);

    await this.sendWakuMessage("offer", offer);
    
    if (this.iceCandidates.length) {
      await this.sendWakuMessage("candidate", this.iceCandidates);
    }
  }

  private async onReadyMessage() {
    console.log("RTC: partner is ready");
  }

  private async sendWakuMessage(type: string, payload: any): Promise<void> {
    const response = await this.node.lightPush.send(this.encoder, {
      payload: utf8ToBytes(JSON.stringify({
        type,
        payload
      }))
    });

    console.log(`sendWakuMessage of type:${type}, with ${response}`);
  }
}