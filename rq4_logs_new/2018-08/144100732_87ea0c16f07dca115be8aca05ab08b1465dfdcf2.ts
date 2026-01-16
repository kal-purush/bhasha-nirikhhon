import AudioController from "./Controllers/AudioController";

const {ccclass, property} = cc._decorator;

@ccclass
export default class Audio extends cc.AudioSource {

    @property isMusic: boolean = false
    
    onLoad () {
        if(this.isMusic)
        {
            AudioController.playMusic(this)
        }
    }
    playSound()
    {
        AudioController.playEffect(this)
    }
}