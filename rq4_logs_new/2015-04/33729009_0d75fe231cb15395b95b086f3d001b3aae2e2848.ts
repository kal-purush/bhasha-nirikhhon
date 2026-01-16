"use strict";
// メインループ - 60 FPS のゲームの場合約 17 ms
// 実行環境によってゲーム速度が変化するので、それを整えるためのFPS
var FPS = 30;
var MSPF = 1000 / FPS;
export default function mainLoop(enterFrame:Function) {
    var startTime = Date.now();
    //処理
    enterFrame();
    // 処理の経過時間
    var deltaTime:number = Date.now() - startTime;
    // 次のループまでのintervalを算出
    var interval = MSPF - deltaTime;
    if (interval > 0) {
        // interval分を待つ
        setTimeout(()=>{
            mainLoop(enterFrame);
        }, interval);
    } else {
        // 次の処理へ
        mainLoop(enterFrame);
    }
}