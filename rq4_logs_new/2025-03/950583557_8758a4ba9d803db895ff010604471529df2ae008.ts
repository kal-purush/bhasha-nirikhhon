

export enum SignalType {  
    BUSY,
    RINGING,
    //CALL_CONNECTING,
}
const busyAudioResource:string = '/busy.mp3';
const ringingAudioResource:string = '/ringing.mp3';
//const connectingAudioResource:string = '/connecting.mp3';

export class AudioSignal{

    private SignalAudio: HTMLAudioElement;
    private playing: boolean = false;

    public constructor(audioElement: HTMLAudioElement){
        this.SignalAudio = audioElement;
    }

    public playSignal(type: SignalType, duration=0): void{
        if(this.playing){
            this.stopSignal();
        }
        switch(type){
            case SignalType.BUSY:
                this.SignalAudio.src = busyAudioResource;
                break;
            case SignalType.RINGING:
                this.SignalAudio.src = ringingAudioResource;
                break;
            /*case SignalType.CALL_CONNECTING:
                this.SignalAudio.src = connectingAudioResource;
                break*/
        }
        this.SignalAudio.play();
        this.playing = true;
        this.SignalAudio.currentTime = 0;
        if (duration === 0){
            this.SignalAudio.loop = true;
        }else{
            this.SignalAudio.loop = false;
            setTimeout(() => {
                this.stopSignal();
            }, duration);
        }
        this.SignalAudio.play().catch(err => console.error("Error playing busy signal:", err));
    }

    public stopSignal(): void{
        this.SignalAudio.pause();
        this.SignalAudio.currentTime = 0;
        this.playing = false;
    }
}