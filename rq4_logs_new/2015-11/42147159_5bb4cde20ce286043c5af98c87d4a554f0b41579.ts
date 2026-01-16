module MiniIntruders {

    export class MainMenu extends Phaser.State {

        // the background
        private background: Phaser.Sprite;

        // the objects involved
        private intro: Phaser.Sprite;
        private logo: Phaser.Sprite;
        private buttons: Phaser.Group;
        private btnPlay: Phaser.Sprite;
        private btnHelp: Phaser.Sprite;
        private btnCredits: Phaser.Sprite;
        private btnExit: Phaser.Sprite;

        // some positions we need to use; calculated on the fly
        private get logoHt() { return this.logo && this.logo.height; }
        private get logoDownY() { return this.world.height - this.logoHt; }
        private get logoUpY() { return this.logoHt*2; }
        private get logoBtnSpace() { return this.logoHt*3; } // space between logo and buttons

        // The tweens
        private tw0IntroFadeOut: Phaser.Tween;
        private tw1LogoFadeIn: Phaser.Tween;
        private tw2LogoMoveDown: Phaser.Tween;
        private tw3LogoMoveUp: Phaser.Tween;

        // Starts the tweens
        private startTweening = () => this.tw0IntroFadeOut.start();

        // updates buttons positioning relative to logo
        private updateButtonsY = () => this.buttons.y = this.logo.y + this.logoBtnSpace;

        public create() {

            // background
            this.background = this.add.sprite(0, 0, 'background01');

            // create our menu elements
            this.createIntro();
            this.createLogo();
            this.createButtons();

            // begin the tweening
            this.chainTweens();
            this.startTweening();


            // TODO menu items, when hovered over change to a highlighted one.


            // TODO register appropriate handlers for menu item clicks?


            // TODO if any key is pressed during the menu animation, skip to end tween State


            // TODO keyboard control of menu?
        }

        /** Chains the tweens together */
        private chainTweens() {
            this.tw0IntroFadeOut.chain(
                this.tw1LogoFadeIn,
                this.tw2LogoMoveDown,
                this.tw3LogoMoveUp);
            // move buttons with logo
            this.tw3LogoMoveUp.onUpdateCallback(this.updateButtonsY);
        }

        /** Create buttons group; they move up with logo, then move back down a bit */
        private createButtons() {
            var y = 0; // y position to keep track of where next button goes
            var spacing = 2.0; // space between buttons (modifier to height)
            var btns = [["btnPlay", "play1.png"], ["btnHelp", "help1.png"], ["btnCredits", "credits1.png"], ["btnExit", "exit1.png"]];
            this.buttons = this.add.group();

            // set initial position
            this.buttons.y = this.logoDownY + this.logoBtnSpace;

            // add each button; anchor at top center, and increment y to account for spacing.
            btns.forEach((val) => {
                var btn: Phaser.Sprite = this.buttons.create(this.world.centerX, y, 'menu');
                btn.anchor.setTo(0.5, 0);
                btn.frameName = val[1];
                y += btn.height*spacing;
                this[val[0]] = btn;
            });

            // final menu item position
            var menuYPos = this.game.world.height/2 - this.buttons.height/2; // center them
        }

        /** Intro starts at center, fades out */
        private createIntro() {
            this.intro = this.add.sprite(this.world.centerX, this.world.centerY, 'menu_intro');
            this.intro.anchor.setTo(0.5, 0.5);
            this.tw0IntroFadeOut = this.add.tween(this.intro).to({alpha: 0});
        }

        /** Logo starts at center, fades in, moves down then moves up */
        private createLogo() {
            this.logo = this.add.sprite(this.world.centerX, this.world.centerY, 'menu_logo');
            this.logo.anchor.setTo(0.5, 0.5);
            this.logo.alpha = 0; // pre-fade in alpha value

            this.tw1LogoFadeIn = this.add.tween(this.logo).to({alpha: 1});
            this.tw2LogoMoveDown = this.add.tween(this.logo).to({y: this.logoDownY});
            this.tw3LogoMoveUp = this.add.tween(this.logo).to({y: this.logoUpY}, 2000);
        }

    }

}