import { EventBus } from "../events/EventBus";
import { EventNames } from "../events/EventNames.enum";

export class PauseScene extends Phaser.Scene {

    constructor() {
        super({key: 'pause'});
    }

    preload() {

    }

    create() {
        this.add.rectangle(0, 0, 1280, 720, 0x000000, 0.7).setOrigin(0, 0);
        this.add.text(1280/2, 720/3, 'Pause menu!').setOrigin(0.5, 0.5);
        const resumeButton = this.add.rectangle(1280/2, (720/2.5), 100, 50, 0xffff00).setOrigin(0.5, 0.5);
        const quitButton = this.add.rectangle(1280/2, (720/2), 100, 50, 0xffffff).setOrigin(0.5, 0.5);
        const resumeText = this.add.text(0, 0, 'Resume', {color: '0x000000'}).setOrigin(0.5, 0.5);
        const quitText = this.add.text(0, 0, 'Quit', {color: '0x000000'}).setOrigin(0.5, 0.5);
        Phaser.Display.Align.In.Center(resumeText, resumeButton);
        Phaser.Display.Align.In.Center(quitText, quitButton);
        
        resumeButton.setInteractive();
        quitButton.setInteractive();

        resumeButton.on('pointerdown', () => {
            EventBus.emit(EventNames.resumeGame);
        });
        
        quitButton.on('pointerdown', () => {
            EventBus.emit(EventNames.exitGame);
        });
        
    }
}