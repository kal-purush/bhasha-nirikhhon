/**
 * Created by Vlad on 9/3/2015.
 */
/**
 * Created by Vlad on 8/18/2015.
 */
///<reference path="../com/DisplaySimple.ts" />
///<reference path="../com/TouchController3.ts" />
class Renderer4{
    private prefixedTransform:string ='webkitTransform';
    // viewPort:HTMLElement;
    posX:number=0;
    posY:number=0;
   // tools:ui.Tools;
    touch:ui.TouchController3;
    building: display.DisplaySimple;
    buildingEl:HTMLElement;
    viewPortEl:HTMLElement;
    viewPort:display.DisplaySimple;

    dots:display.DisplaySimple;
    consol:JQuery

    swap:boolean = false;
    constructor(){
        this.consol=$('#Console');

        this.buildingEl =  document.getElementById('Building');
        this.viewPortEl =  document.getElementById('ViewPort');
        this.building = new display.DisplaySimple(this.buildingEl,'webkitTransform','webkitTransformOrigin','building');

        this.viewPort = new display.DisplaySimple(this.viewPortEl,'webkitTransform','webkitTransformOrigin','viewport');
        this.viewPort.applyReg();

       // this.building.drawCenter();
        var ar:any[]=[{x:100,y:100},{x:800,y:100},{x:800,y:800},{x:100,y:800}]

        for (var i = 0, n = ar.length; i < n; i++) {
            var p = ar[i];
            $('<dot>').addClass('dot').text('x: '+p.x+' y: '+p.y).css({left:p.x,top:p.y}).appendTo(this.building.$view);

        }

        // this.viewPort = document.getElementById('ViewPort');
        this.dots = new display.DisplaySimple(document.getElementById('Dots'),'webkitTransform','webkitTransformOrigin','dotA');

        // $('#Content').click((evt)=>this.onViewPortClick(evt));
       // this.tools = new ui.Tools();

        this.touch = new ui.TouchController3(this.viewPortEl,$('#Indicator'));
        this.touch.onMoveStart=()=>this.onMoveStart();
        this.touch.onMoveEnd = ()=>this.onMoveEnd();

        this.touch.onGestStart =()=>this.onGestStart();
        this.touch.onGestStop =()=>this.onGestStop();
        this.curAng=0;
        this.curScale=1;
        // this.setCenter(200,200);

        //this.onAnimationFrame(0);
    }

    setCenter(x:number,y:number):void{
        this.posX=x;
        this.posY=y;
        this.building.setCenter(x,y);
    }
    onGestStart():void{
        // console.log(' onGestStart ');
        this.startAng = this.curAng;
        this.startScale = this.curScale;
        var cX = this.touch.centerX;
        var cy = this.touch.centerY;
        this.stopG = false;
        this.startGest();
        // this.onAnimationFrame(0);
        this.calcZR();
        this.start();
    }

    onMoveStart():void{
        this.startX = this.posX;
        this.startY = this.posY;
        //this.startPos();
        this.calcPosition();
        this.start();
        //  setTimeout(()=>this.calcPosition(),20);
    }

    onMoveEnd():void{
        this.stop();
    }

    onGestStop():void{
        this.stopG = true;
        this.stop();
    }

    private startX:number=0;
    private startY:number=0;

    private startScale:number=1;
    private startAng:number=0;
    private curScale:number=1;
    private curAng:number=0;




    private total:number=0;
    private last:number=0;
    private count:number=0;

    cFPS(){
        var now = new Date().getTime();
        var fps = 1000/(now-this.last);
        this.total+=fps;
        this.last=now;
        this.count++
        if(this.count==30){
            this.count=0;
            $('#FPS').text((this.total/30).toFixed());
            this.total=0;

        }
    }

    start():void{
        if(!this.isActive){
            this.isActive = true;
            this.onAnimationFrame(0);
        }
        clearTimeout(this.timeout);

    }

    timeout:number;
    stop():void{
        clearTimeout(this.timeout);
        this.timeout = setTimeout(()=>{
            $('#Move').show();
            this.isActive = false;

        },5000)
    }

    isActive;

    onAnimationFrame(st:number){
        this.cFPS();
        if(this.isActive)requestAnimationFrame((st)=>this.onAnimationFrame(st));
        this.render();
    }

    timer:number;
    stopTimer():void{
        clearInterval(this.timer)
    }

    isPoz:boolean;
    isRS:boolean;

    dAngle:number;

    stopG:boolean
    calcZR():void{
        var z = (this.touch.zoom*this.curScale)

        this.curScale = this.startScale +(z*0.001);

        var ga:number = this.startAng + this.touch.angle;
        var da = ga - this.curAng;
       if(this.stopG && Math.abs(da)<1)this.stopGest();

        if(Math.abs(da)>100)this.curAng=ga;
        else this.curAng+=(da*0.2);

        //  this.consol.text(this.curScale+'   '+this.curAng);
    }

    startGest():void{
        this.isRS = true;
        $('#Gestur2').show();
    }

    stopGest():void{
        this.isRS = false;
        $('#Gestur2').hide();
    }
    stopPos():void{
        this.isPoz=false;
        $('#Move2').hide();
    }
    startPos():void{
        this.isPoz = true;
        $('#Move2').show();
    }


    calcPosition():void{
        var gx =this.startX-this.touch.DX;
        var gy=this.startY-this.touch.DY;
        var dx = gx-this.posX;
        var dy = gy-this.posY;
        if(Math.sqrt((dx*dx) + (dy*dy))<0.1){
            this.stopPos();
        }else{
            this.startPos();
            this.posX+=dx*0.1;
            this.posY+=dy*0.1;
        }
    }


    render():void{
        this.calcPosition();

        if(this.isPoz){
          // console.log(this.posX,this.posY);


          //  var p:view.Point = this.building.toLocal(this.posX,this.posY);
//console.log(p);
         this.building.setCenter(this.posX,this.posY);

        // this.viewPort.move(this.posX,this.posY);
           // if(this.swap)this.buildingEl.style[this.prefixedTransform]= 'translate('+this.posX+'px,'+this.posY+'px) rotate(0) scale(1) translateZ(0)';
           // else  this.viewPortEl.style[this.prefixedTransform]= 'translate('+this.posX+'px,'+this.posY+'px) rotate(0) scale(1) translateZ(0)';
        }

        if(this.isRS){
            this.stopPos();
            this.calcZR();
            this.building.setAS(this.curAng,this.curScale);
           // this.building.readMatrixAr();
           // if(this.swap)this.viewPortEl.style[this.prefixedTransform]= 'translate(0,0) rotate('+this.curAng+'deg) scale('+this.curScale+') translateZ(0)'
           // else this.buildingEl.style[this.prefixedTransform]= 'translate(0,0) rotate('+this.curAng+'deg) scale('+this.curScale+') translateZ(0)';
        }


    }
}





