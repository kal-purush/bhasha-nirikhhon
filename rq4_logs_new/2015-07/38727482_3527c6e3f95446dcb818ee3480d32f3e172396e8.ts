module managers {
    export class Asset {
        // PUBLIC PROPERTIES
        public loader: createjs.LoadQueue;
        public atlas: createjs.SpriteSheet;

        //Private Properties
       private manifest = [
        { id: "wood", src: "assets/images/wood.jpg" },
        { id: "backgroundMusic", src: "assets/audio/backgroundMusic.ogg" },
        { id: "catsound", src: "assets/audio/catsound.ogg" },
        { id: "meow", src: "assets/audio/meow.ogg" }
       ];

       private data = {
           "images": [
               "assets/images/atlas.png"
           ],

           "frames": [
               [2, 2, 144, 89, 0, -1, -12],
               [148, 2, 50, 50, 0, 0, 0],
               [148, 54, 50, 50, 0, 0, 0]
           ],

           "animations": {
               "cat": [0],
               "cheese": [1],
               "mouse": [2]
           }
       }

       // CONSTRUCTOR
       constructor() {
           this.preload();
       }

       preload() {
           this.loader = new createjs.LoadQueue();
           this.loader.installPlugin(createjs.Sound);
           // event listener triggers when assets are completely loaded
           this.loader.on("complete", init, this);
           this.loader.loadManifest(this.manifest);

           // create texture atlas
           this.atlas = new createjs.SpriteSheet(this.data);
       }
    }
} 