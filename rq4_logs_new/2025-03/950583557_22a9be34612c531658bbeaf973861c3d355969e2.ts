import { WakuPhoneMessage, WakuPhoneMessage_MessageType } from "./waku-phone-message";

enum WakuPhoneCallState {
    IDLE = "IDLE",
    CALLING = "CALLING",
    RINGING = "RINGING",
    IN_CALL = "IN_CALL",
    ENDED = "ENDED",
}

class WakuPhoneCall {
    private state: WakuPhoneCallState = WakuPhoneCallState.IDLE;
    private callId: string;
    private callerPeerId: string;
    private calledPeerId: string;
    private callerDisplayName: string | undefined;
    private calledDisplayName: string | undefined;

    constructor(callerPeerId: string, calledPeerId: string, callerDisplayName?: string, calledDisplayName?: string) {
        this.callId = this.generateCallId();
        this.callerPeerId = callerPeerId;
        this.calledPeerId = calledPeerId;
        this.callerDisplayName = callerDisplayName;
        this.calledDisplayName = calledDisplayName;
      }
    
    private generateCallId(): string {
        return Math.random().toString(36).substring(2, 15);
    }

    private async transition(messageType: WakuPhoneMessage_MessageType): Promise<void> {
        switch (messageType) {
            case WakuPhoneMessage_MessageType.DIAL:
                console.log(this.callId + " Dialing...");
                this.state = WakuPhoneCallState.CALLING;
                break;
            case WakuPhoneMessage_MessageType.RINGING:
                console.log(this.callId + " Ringing...");
                this.state = WakuPhoneCallState.RINGING;
                break;
            case WakuPhoneMessage_MessageType.ANSWER:
                console.log(this.callId + " Answered.");
                this.state = WakuPhoneCallState.IN_CALL;
                break;
            case WakuPhoneMessage_MessageType.REJECT:
                console.log(this.callId + " Call rejected.");
                this.state = WakuPhoneCallState.ENDED;
                break;
            case WakuPhoneMessage_MessageType.BYE:
                console.log(this.callId + " Call ended.");
                this.state = WakuPhoneCallState.ENDED;
                break;
            default:
                console.error(this.callId + " Unknown message type:", messageType);
        }
    }

    public handleMessage(message: WakuPhoneMessage): void {
        // Call ID mismatch check
        if (message.callId.toString() !== this.callId) {
            console.error("Call ID mismatch:", message.callId, this.callId);
            return;
        }

        // Display names can be updated
        if (this.callerDisplayName == undefined && message.callerDisplayName !== undefined) {
            this.callerDisplayName = message.callerDisplayName;
        }

        if (this.calledDisplayName == undefined && message.calledDisplayName !== undefined) {
            this.calledDisplayName = message.calledDisplayName;
        }
    
        // Handle the message type
        this.transition(message.messageType).then(() => {
            console.log("Transitioned to state:", this.state);
        }).catch((error) => {
            console.error("Error transitioning state:", error);
        });

        if (message.webrtcData !== undefined) {
            console.log("WebRTC data:", message.webrtcData);
            // Handle WebRTC data here
            // Presumable we can emit webrtcData here to be handled by the caller
        }
    }
}