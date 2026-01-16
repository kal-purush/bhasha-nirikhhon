import { ILayer } from "types/state";



class Layer implements ILayer {

    name: string;

    // 塗ってあるかフラグの 2 次元配列
    tiles: boolean[][] = [];

    // 色
    color: string;

    // 階層
    floor: number;
    height: number;

    visibility: boolean = true;

    /**
     * コンストラクタ
     * @param index レイヤーのインデックス
     * @param gridWidth グリッドの横幅
     * @param gridHeight グリッドの縦幅
     */
    constructor(index: number, gridWidth: number, gridHeight: number) {
        this.name = 'Layer ' + index;

        this.color = '#' + (Math.random() * 3000000).toString(16);

        var rangeRndm = function (min: number, max: number) {
            if (max) {
                return Math.random() * (max - min + 1) + min | 0;
            } else {
                return Math.random() * min | 0;
            }
        };

        this.color = 'hsl(' + rangeRndm(0, 360) + ', 100%, ' + rangeRndm(25, 75) + '%)';

        this.resize(gridWidth, gridHeight);
    }

    /**
     * グリッドをリサイズする
     * @param w 横幅
     * @param h 縦幅
     */
    resize(w: number, h: number) {

        // bool の 2 次元配列
        const tiles: boolean[][] = [];


        for (let y = 0; y < h; ++y) {

            const row: boolean[] = [];

            for (let x = 0; x < w; ++x) {

                // 
                const value = (this.tiles.length > y && this.tiles[y].length > x) ? this.tiles[y][x] : false;



                // デバッグ
                row.push(Math.random() > 0.5);
                // row.push(value);

            }

            tiles.push(row);

        }

        this.tiles = tiles;

    }

}



export default Layer;