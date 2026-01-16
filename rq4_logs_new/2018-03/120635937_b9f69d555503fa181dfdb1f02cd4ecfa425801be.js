// Learn cc.Class:
//  - [Chinese] http://www.cocos.com/docs/creator/scripting/class.html
//  - [English] http://www.cocos2d-x.org/docs/editors_and_tools/creator-chapters/scripting/class/index.html
// Learn Attribute:
//  - [Chinese] http://www.cocos.com/docs/creator/scripting/reference/attributes.html
//  - [English] http://www.cocos2d-x.org/docs/editors_and_tools/creator-chapters/scripting/reference/attributes/index.html
// Learn life-cycle callbacks:
//  - [Chinese] http://www.cocos.com/docs/creator/scripting/life-cycle-callbacks.html
//  - [English] http://www.cocos2d-x.org/docs/editors_and_tools/creator-chapters/scripting/life-cycle-callbacks/index.html

cc.Class({
    extends: cc.Component,

    properties: {
        btn_on: {
            default: null,
            type: cc.Node
        },
        btn_off: {
            default: null,
            type: cc.Node
        },
        audio: {
            url: cc.AudioClip,
            default: null
        },
    },

    onLoad () {
        
    },

    start () {
        this.current = cc.audioEngine.play(this.audio, true, 1);
    },

    // update (dt) {},

    SoundOn : function() {
        this.btn_on.active = true;
        this.btn_off.active = false;
        cc.audioEngine.setVolume(this.current, 1);
    },

    SoundOff : function() {
        this.btn_on.active = false;
        this.btn_off.active = true;
        cc.audioEngine.setVolume(this.current, 0);
    },
});