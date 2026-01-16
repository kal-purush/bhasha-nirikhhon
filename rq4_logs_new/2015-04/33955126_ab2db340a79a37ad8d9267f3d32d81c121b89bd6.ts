/// <reference path="types/snapsvg.d.ts" />

class Base {
    /**
     * Main game paper
     */
    $gamePaper;

    /**
     * Base control element of the game
     */
    $base;

    constructor(gameID: string) {
        var wHeight:number;

        // Create main game paper
        this.$gamePaper = Snap( gameID );

        // calculate height of the paper
        wHeight = window.innerHeight - 10;
        this.$gamePaper.node.style.height = wHeight + '.px';


        this.drawBase();
    }

    /**
     * Drawing the main control element of the game
     */
    drawBase() {
        this.$base = this.$gamePaper.circle(150, 150, 100);
    }

}