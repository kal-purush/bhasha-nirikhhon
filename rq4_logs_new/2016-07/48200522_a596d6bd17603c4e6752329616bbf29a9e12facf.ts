namespace United {

    export namespace Audio {

        export class Controller {

            private static controllerStore : { [key:string] : United.Audio.Controller } = {};

            private store: Sup.Audio.SoundPlayer[];
            public name: string;
            public defaultVolume: number;
            public volume: number;

            constructor(name: string,defaultVolume : number) {
                this.store = [];
                this.name = name;
                this.defaultVolume = Utils.clampNumber(defaultVolume,[0,1]);
                this.volume = this.defaultVolume;
                United.Audio.Controller.controllerStore[this.name] = this;
            }

            linkSound(SoundAsset: Sup.Audio.SoundPlayer) : void {
                this.store.push(SoundAsset);
            }

            public static setVolume(name: string,volume: number) : boolean {
                if(Utils.has(United.Audio.Controller.controllerStore,name)) {
                    return United.Audio.Controller.controllerStore[name].setVolume(volume);
                }
                return false;
            }

            setVolume(volume: number) : boolean {
                this.volume = Utils.clampNumber(volume,[0,1]);
                this.store.forEach( (SoundAsset : Sup.Audio.SoundPlayer) => {
                    SoundAsset.setVolume(this.volume);
                });
                return true;
            }

            reset(store?:boolean) : void {
                this.setVolume(this.defaultVolume);
                if(store) {
                    this.store = [];
                }
            }

        }

        export class Core {

            private static store : { [key:string] : United.Audio.Playlist} = {};
            public static active : string;

            public static addPlaylist(name: string,playlist: United.Audio.Playlist) {
                this.store[name] = playlist;
            }

            public static setPlaylist(name: string,trackID?: number) : void {
                if(this.store[name]) {
                    if(this.active) {
                        this.store[this.active].stop();
                        this.store[name].start();
                        this.active = name;
                    }
                    else {
                        this.active = name;
                        this.store[name].start();
                    }
                }
            }

            public static next() {
                this.store[this.active].next();
            }

            public static prev() {
                this.store[this.active].prev();
            }

            public static update() {
                if(this.active)
                    this.store[this.active].update();
            }

        }

        export type trackOption = {
            asset : string,
            title? : string
            defaultVolume? : number
        }

        export interface playlistOption {
            defaultTrack?: number
            autoStart?: boolean
            startRandom?: boolean
        }

        export class Playlist extends United.Addons {

            private activeTrack : number;
            public isPlaying : boolean = false;
            private soundInstance : Sup.Audio.SoundPlayer;
            private playlistSize : number;

            public startRandom : boolean = false;

            constructor(private tracksList : { [key:number] : United.Audio.trackOption },options?: playlistOption) {
                super();
                this.playlistSize = Utils.objectSize(this.tracksList);
                let track : number;
                options = options || {};
                if(options.defaultTrack && !options.startRandom) {
                    track = options.defaultTrack;
                }
                else if(options.startRandom) {
                    track = Utils.getRandom(0,this.playlistSize);
                    this.startRandom = true;
                }
                else {
                    track = 0;
                }
                this.setTrack(track);

                if(options.autoStart) {
                    this.start();
                }
            }

            public getTitle(ID?: number) : string {
                if(ID) {
                    if(Utils.has(this.tracksList,ID)) {
                        return this.tracksList[ID].title || "Undefined title!";
                    }
                    return "Undefined Track ID!";
                }
                return this.tracksList[this.activeTrack].title || "Undefined title!";
            }

            private setTrack(ID: number,start?:boolean) : void {
                const track : trackOption = this.tracksList[ID];
                this.activeTrack = ID;
                if(this.soundInstance) {
                    this.soundInstance.stop();
                }
                this.soundInstance = new Sup.Audio.SoundPlayer(track.asset,track.defaultVolume || 1.0,{loop: false});
                if(start) {
                    this.soundInstance.play();
                }
            }

            public start() : void {
                this.isPlaying = true;
                this.soundInstance.play();
            }

            public playTrack(trackID?: number) : void {
                if(trackID) {
                    this.setTrack(trackID);
                }
                else {
                    this.next();
                }
            }

            public stop() : void {
                this.isPlaying = false;
                this.soundInstance.pause();
            }

            public resume() : void {
                this.isPlaying = true;
                this.soundInstance.play();
            }

            public next() : void {
                let nextID : number = this.activeTrack + 1;
                if(!this.tracksList[nextID]) {
                    nextID = 0;
                }
                this.setTrack(nextID,true);
            }

            public prev() : void {
                let prevID : number = this.activeTrack - 1;
                if(!this.tracksList[prevID]) {
                    prevID = this.playlistSize;
                }
                this.setTrack(prevID,true);
            }

            public update() : void {
                if(this.isPlaying) {
                    if(this.soundInstance.getState() == Sup.Audio.SoundPlayer.State.Stopped) {
                        this.next(); // Prochaine piste!
                    }
                }
            }

        }

    }

    export module StackControls {

        export interface ActionConstructor {
            keyboard?: string[];
            mButton?: number[];
            gamepad?: number[];
            gButton?: number[];
            gAxis?: number[];
            gTrigger?: number[];
        }

        export class Action {
            keyboard : string[];
            mButton : number[];
            gamepad : number[];
            gButton : number[];
            gAxis : number[];
            gTrigger : number[];

            constructor(touches : ActionConstructor) {
                this.keyboard = touches.keyboard ? touches.keyboard : [];
                this.mButton = touches.mButton ? touches.mButton : [];
                this.gamepad = touches.gamepad ? touches.gamepad : [];
                this.gButton = touches.gButton ? touches.gButton : [];
                this.gAxis = touches.gAxis ? touches.gAxis : [];
                this.gTrigger = touches.gTrigger ? touches.gTrigger : [];
            }

            isDown() : boolean {
                if (this.keyboard) {
                    const klength : number = this.keyboard.length;
                    for (let i : number = 0; i < klength; i++)
                        if (Sup.Input.isKeyDown(this.keyboard[i])) return true;
                }

                if (this.gamepad) {
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        if (this.gButton) {
                        let gblength = this.gButton.length;
                        for (let j = 0; j < gblength; j++)
                            if (Sup.Input.isGamepadButtonDown(this.gamepad[i], this.gButton[j])) return true;
                        }
                    }
                }

                if (this.mButton) {
                    const mlength : number = this.mButton.length;
                    for (let i : number = 0; i < mlength; i++)
                        if (Sup.Input.isMouseButtonDown(this.mButton[i])) return true;
                }

                return false;
            }

            isAllDown() : boolean {
                let flag : { keyboard : boolean, gamepad : boolean , mouse : boolean } = {
                    keyboard: true,
                    gamepad: true,
                    mouse: true
                }

                if (this.keyboard) {
                    const klength : number = this.keyboard.length;
                    for (let i : number = 0; i < klength; i++)
                        if (!Sup.Input.isKeyDown(this.keyboard[i])) {
                            flag.keyboard = false;
                            break;
                        }
                }

                if (this.gamepad) {
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        if (this.gButton) {
                            const gblength : number = this.gButton.length;
                            for (let j = 0; j < gblength; j++)
                                if (!Sup.Input.isGamepadButtonDown(this.gamepad[i], this.gButton[j])) {
                                    flag.gamepad = false;
                                    break;
                                }
                        }
                    }
                }

                if (this.mButton) {
                    const mlength : number = this.mButton.length;
                    for (let i : number = 0; i < mlength; i++)
                        if (!Sup.Input.isMouseButtonDown(this.mButton[i])) {
                            flag.mouse = false;
                            break;
                        }
                }

                return flag.mouse && flag.keyboard && flag.gamepad;
            }

            wasJustPressed(options?: { autoRepeat?: boolean; }) : boolean {
                if (this.keyboard) {
                    const klength : number = this.keyboard.length;
                    for (let i : number = 0; i < klength; i++)
                        if (Sup.Input.wasKeyJustPressed(this.keyboard[i], options)) return true;
                }

                if (this.gamepad) {
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        if (this.gButton) {
                            const gblength : number = this.gButton.length;
                            for (let j = 0; j < gblength; j++)
                                if (Sup.Input.wasGamepadButtonJustPressed(this.gamepad[i], this.gButton[j])) return true;
                        }
                    }
                }

                if (this.mButton) {
                    const mlength : number = this.mButton.length;
                    for (let i : number = 0; i < mlength; i++)
                        if (Sup.Input.wasMouseButtonJustPressed(this.mButton[i])) return true;
                }

                return false;
            }

            wasJustReleased() : boolean {
                if (this.keyboard) {
                    const klength : number = this.keyboard.length;
                    for (let i = 0; i < klength; i++)
                        if (Sup.Input.wasKeyJustReleased(this.keyboard[i])) return true;
                }

                if (this.gamepad) {
                    const gplength : number = this.gamepad.length;
                    for (let i = 0; i < gplength; i++) {
                        if (this.gButton) {
                            const gblength : number = this.gButton.length;
                            for (let j = 0; j < gblength; j++)
                                if (Sup.Input.wasGamepadButtonJustReleased(this.gamepad[i], this.gButton[j])) return true;
                        }
                    }
                }

                if (this.mButton) {
                    const mlength : number = this.mButton.length;
                    for (let i = 0; i < mlength; i++)
                        if (Sup.Input.wasMouseButtonJustReleased(this.mButton[i])) return true;
                }

                return false;
            }

            getAxisValues() : number[][] {
                if (this.gamepad) {
                    const ret : any[] = []
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        ret[i] = [];
                        if (this.gButton) {
                            const galength : number = this.gAxis.length;
                            for (let j : number = 0; j < galength; j++)
                                ret[i][j] = Sup.Input.getGamepadAxisValue(this.gamepad[i], this.gAxis[j]);
                        }
                    }
                    return ret;
                }
                return undefined;
            }

            getTriggersValues() : any {
                if (this.gamepad) {
                    const ret : any[] = []
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        ret[i] = [];
                        if (this.gTrigger) {
                            const gtlength : number = this.gTrigger.length;
                            for (let j : number = 0; j < gtlength; j++)
                                ret[i][j] = Sup.Input.getGamepadAxisValue(this.gamepad[i], this.gTrigger[j]);
                        }
                    }
                    return ret;
                }
                return undefined;
            }

            addKey(periphType : string, newKey : string | number) {
                this[periphType].push(newKey);
            }

            editKey(periphType : string, oldKey : string | number, newKey : string | number) {
                const indexKey : number = this[periphType].indexOf(oldKey);
                this[periphType][indexKey] = newKey;
            }

            removeKey(periphType : string, oldKey : string | number) {
                let indexKey = this[periphType].indexOf(oldKey);
                delete this[periphType][indexKey];
            }
        }

        export abstract class Controls {

            constructor (ctrl : { [key:string] : Action }) {
                for (const key in ctrl) {
                    this[key.toUpperCase()] = ctrl[key];
                }
            }

            addKey(actionName : string, periphType : string, newKey : string | number) {
                this[actionName.toUpperCase()].addKey(periphType, newKey);
            }

            editKey(actionName : string, periphType : string, oldKey : string | number, newKey : string | number) {
                this[actionName.toUpperCase()].editKey(periphType, oldKey, newKey);
            }

            removeKey(actionName : string, periphType : string, oldKey : string | number) {
                this[actionName.toUpperCase()].removeKey(periphType, oldKey);
            }

            addAction(actionName : string, action : Action) {
                this[actionName.toUpperCase()] = action;
            }

            editAction(actionName : string, action : string[]) {
                this[actionName.toUpperCase()] = action;
            }

            renameAction(oldName : string, newName : string) {
                oldName = oldName.toUpperCase();
                this[newName.toUpperCase()] = this[oldName];
                delete this[oldName];
            }

            removeAction(actionName : string) {
                delete this[actionName.toUpperCase()];
            }

        }

    }

    export class KeyAction {

        constructor(private keys: string[]) {}

        public pressed(onlyOne: boolean = true) : boolean {
            let ret : boolean = true;
            this.keys.forEach( (key: string) => {
                if(Sup.Input.wasKeyJustPressed(key)) {
                    if(onlyOne) return true;
                }
                else {
                    ret = false;
                }
            });
            return ret;
        }

        public released(onlyOne: boolean = true) {
            let ret : boolean = true;
            this.keys.forEach( (key: string) => {
                if(Sup.Input.wasKeyJustPressed(key)) {
                    if(onlyOne) return true;
                }
                else {
                    ret = false;
                }
            });
            return ret;
        }

        public down(onlyOne: boolean = true) {
            let ret : boolean = true;
            this.keys.forEach( (key: string) => {
                if(Sup.Input.wasKeyJustPressed(key)) {
                    if(onlyOne) return true;
                }
                else {
                    ret = false;
                }
            });
            return ret;
        }

    }

    export function Key(...keys: string[]) : United.KeyAction {
        return new United.KeyAction(keys);
    }

    export class Mouse {

        // Static short-cut
        public static get left() : boolean {
            return Sup.Input.wasMouseButtonJustPressed(0);
        }

        public static get middle() : boolean {
            return Sup.Input.wasMouseButtonJustPressed(1);
        }

        public static get right() : boolean {
            return Sup.Input.wasMouseButtonJustPressed(2);
        }

    }

}