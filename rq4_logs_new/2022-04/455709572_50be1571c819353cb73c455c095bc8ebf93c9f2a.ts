import { GlobalConstants } from "../global-constants";

export class IconSelect extends Phaser.Scene {

    constructor () {
        super('icons')
    }

    preload() {
        // Used for preloading assets into your scene, such as
        // - images
        // - sounds
        this.load.image('AwesomeFace', 'assets/sprites/awesomeface.png')
        this.load.image('magikarp', 'assets/sprites/magikarp.png')
        this.load.image('fuzzy', 'assets/sprites/fuzzy.png')
        this.load.image('lady', 'assets/sprites/lady.png')
        this.load.image('mage', 'assets/sprites/mage.png')
        this.load.image('purpledude', 'assets/sprites/purpledude.png')
        this.load.image('shovelknight', 'assets/sprites/shovelknight.png')
        this.load.image('slime', 'assets/sprites/slime.png')
    }

    create() {
        // Creates Text title
        let titleText = "Character Selection"

        let title = this.add.text(300, 50, titleText, { font: '64px Arial Black'});
        title.setTint(0xff00ff, 0xffff00, 0x0000ff, 0xff0000);

        // Creates images
        const awesomeface = this.add.image(500, 300, 'AwesomeFace');
        const magikarp = this.add.image(750, 300, 'magikarp');
        const fuzzy = this.add.image(250, 300, 'fuzzy');
        const lady = this.add.image(1000, 300, 'lady');
        const mage = this.add.image(500, 600, 'mage');
        const purpledude = this.add.image(750, 600, 'purpledude');
        const shovelknight = this.add.image(250, 600, 'shovelknight');
        const slime = this.add.image(1000, 600, 'slime');

        // Sets image scales to comparable size
        awesomeface.setScale(0.2);
        magikarp.setScale(0.15);
        fuzzy.setScale(0.2);
        lady.setScale(0.25);
        mage.setScale(0.05);
        purpledude.setScale(0.15);
        shovelknight.setScale(0.4);
        slime.setScale(0.3);

        // Makes images into buttons, when clicked it moves game to intro scene
        awesomeface.setInteractive({cursor: GlobalConstants.ButtonCursor})
            .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {

                this.scene.start('intro', { image: 'awesomeface.png' });
            }) 

        magikarp.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'magikarp.png' });
        }) 

        fuzzy.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'fuzzy.png' });
        }) 

        lady.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'lady.png' });
        }) 

        mage.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'mage.png' });
        }) 

        purpledude.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'purpledude.png' });
        }) 

        shovelknight.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'shovelknight.png' });
        }) 

        slime.setInteractive({cursor: GlobalConstants.ButtonCursor})
        .on(Phaser.Input.Events.GAMEOBJECT_POINTER_DOWN, () => {
            this.scene.start('intro', { image: 'slime.png' });
        }) 
    }
}

