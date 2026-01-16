// Source file name: button.ts
// Author: Louis Smith
// Last modified by: Louis Smith
// Last modified date: 25/02/2015
// Description: This code is the only way I was able to get the music playing

module managers {
    // Image and Sound Manifest;
    var assetManifest = [
        { id: "music", src: "assets/sounds/background_music.mp3" }
    ];
    export class Assets {
        public static manifest;
        public static data;

        public static loader;
        public static atlas: createjs.SpriteSheet;
        public static lightningAtlas: createjs.SpriteSheet;
        public static bitMapFont: createjs.SpriteSheet;

        public static init() {
            createjs.Sound.initializeDefaultPlugins();
            createjs.Sound.alternateExtensions = ["mp3"];
            this.loader = new createjs.LoadQueue();
            this.loader.installPlugin(createjs.Sound);
            this.loader.loadManifest(assetManifest);
        }

    }
}