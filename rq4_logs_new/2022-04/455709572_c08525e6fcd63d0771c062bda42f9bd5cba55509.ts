import { GlobalConstants } from "../global-constants";
import { Player } from "../models/player";
import { ButtonLoaderService } from "../services/button-loader.service";
import { JobGeneratorService } from "../services/job-generator.service";

export class IntroScene extends Phaser.Scene {
    player: Player;
    constructor() {
        super({key: 'intro'});
        let jobGenerator = new JobGeneratorService();
        let job = jobGenerator.generateRandomJob();
        this.player = new Player(job, 1000);
    }

    create() {
        let titleText = "Adulting Sim!"
        let introText = `You just started your first job as a ${this.player.job.title} at ${this.player.job.company} making $${this.player.job.monthlySalary} a month.\n\n` + 
        `You have $${this.player.cash} to your name.\n\n` +
        `You have moved out of your parent's house and into your first apartment.\n\n` +
        `Rent is due on the 1st.\n\n`+
        `It's up to YOU now to decide how to manage your money. Do you think you have what it takes to survive as an adult?\n\n` +

        `Let's get started by making your first budget!`;

        let intro = this.add.text(50, 100, introText)
            .setAlign("left")
            .setFontFamily(GlobalConstants.TextFont)
            .setWordWrapWidth(this.cameras.main.width / 2)
            .setDepth(100);

        let screenCenterX = this.cameras.main.worldView.x + this.cameras.main.width / 2;
        let screenCenterY = this.cameras.main.worldView.y + this.cameras.main.height / 2;
        let playerPosX = screenCenterX + this.cameras.main.width / 4;
        let playerPosY = screenCenterY - (this.cameras.main.height / 4);

        // add player logo
        this.add.sprite(playerPosX, playerPosY, 'player')
            .setTint()
            .setDisplaySize(300, 300);

        // add continue button
        let continueButton = this.add.sprite(screenCenterX, (this.cameras.main.height - this.cameras.main.height / 4), "btn-Continue")
            .setTint(GlobalConstants.ButtonTint)
            .setInteractive({cursor: GlobalConstants.ButtonCursor})
            .setScale(0.5, 0.5)
            .on('pointerup', ()=> {
                this.scene.start('main', {player: this.player});
            });
            continueButton.on('pointerover', () => {
                // TOOD: figure out how to chain this to the initialization
                // ...cant figure it out without using 'this' but its scoped 
                // to the scene, not the button
                continueButton.setTint(GlobalConstants.ButtonHoverTint);
            });
            continueButton.on('pointerout', () => {
                continueButton.setTint(GlobalConstants.ButtonTint);
            });
    }

    preload() {
        let buttonLoaderService = new ButtonLoaderService();
        buttonLoaderService.loadButton("Continue", this);
        this.load.image('player', location.href + 'assets/sprites/awesomeface.png');
    }

    override update() {

    }
}