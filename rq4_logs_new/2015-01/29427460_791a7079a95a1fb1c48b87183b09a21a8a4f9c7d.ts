/// <reference path="Class.ts" />
module hh{
    var _isRunning:boolean = false;
    export function mainLoop(cb, ctx?:any){
        if(_isRunning){
            warn(logCode.w_1);
            return;
        }
        _isRunning = true;
        var requestAnimFrame:Function = window["requestAnimationFrame"] ||
            window["webkitRequestAnimationFrame"] ||
            window["mozRequestAnimationFrame"] ||
            window["oRequestAnimationFrame"] ||
            window["msRequestAnimationFrame"];

        var preTime = Date.now();
        if (requestAnimFrame != null) {
            var callback = function () {
                var curTime = Date.now();
                var frameTime = curTime - preTime;
                preTime = curTime;
                cb.call(ctx, frameTime);
                requestAnimFrame(callback);
            };
            requestAnimFrame(callback);
        } else {
            var callback = function () {
                var curTime = Date.now();
                var frameTime = curTime - preTime;
                preTime = curTime;
                cb.call(ctx, frameTime);
            };
            setInterval(callback, (1/60) * 1000);
        }
    }
}