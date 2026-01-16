///<reference path="../lib/pixi.d.ts" />

module towngame {
    export class TileMaker {
        public static MakeTiles(cb) {
            window.tiles = [];

            //var spritesheet:PIXI.Texture = PIXI.Texture.fromImage("img/roguelikeSheet_transparent.png");
            var spritesheet:PIXI.Texture = PIXI.Texture.fromImage("img/TileA5.png");

            console.log("Making " + (spritesheet.width / 32) * (spritesheet.height / 32) + " tiles");

            for (var y = 0; y < (spritesheet.height / 32); y++) {
                window.tiles[y] = [];
                for (var x = 0; x < (spritesheet.width / 32); x++) {
                    window.tiles[y][x] = new PIXI.Texture(spritesheet, new PIXI.Rectangle(x * 32, y * 32, 32, 32));
                }
            }

            cb();
        }
    }
}