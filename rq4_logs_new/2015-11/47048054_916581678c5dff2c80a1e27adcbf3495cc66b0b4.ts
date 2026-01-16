///<reference path="../lib/pixi.d.ts" />

module towngame {
    export class AssetLoader {
        public static LoadAssets(callback: Function) {
            var assetsToLoad = [
                "img/logo.png", "img/hackwestern_logo.png", "maps/sample_indoor.tmx", "maps/sample_map.tmx",
                "img/clouds.jpg","img/musicon.png","img/musicoff.png","img/wrench.png","img/yellowSheet.png"
            ];
            assetsToLoad.forEach((asset) => PIXI.loader.add(asset));
            PIXI.loader.once('complete', callback);
            PIXI.loader.load();
        }
    }
}