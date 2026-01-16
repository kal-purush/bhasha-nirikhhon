module hh{
    var tickCount:number = 0;
    var maxDetalTime:number = 500;
    var totalDetalTime:number = 0;
    var fpsEle;
    export function profile(frameTime:number){
        tickCount++;
        totalDetalTime += frameTime;
        if(totalDetalTime >= maxDetalTime){
            var fps = Math.floor(tickCount * 1000 / totalDetalTime);
            tickCount = 0;
            totalDetalTime = 0;
            if(!fpsEle){
                fpsEle = document.getElementById("fps");
            }
            if(fpsEle){
                fpsEle.innerHTML = fps + "";
            }
        }

    }
}