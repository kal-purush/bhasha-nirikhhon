/**
 * Created by Vlad on 9/4/2015.
 */
    ///<reference path="../libs/typings/jquery.d.ts" />
    ///<reference path="Geom.ts" />

module display{
    import Point  = geom.Point;

    export class DisplaySimple{
        parent:DisplaySimple;
        posX:number=0;
        posY:number=0;
        scaleX:number=1;
        scaleY:number=1;
        scale:number=1;
        //  rotation:number=0
        angle:number=0;
        skewX:number=0;
        skewY:number=0;
        regX:number=0;
        regY:number=0;
        center:JQuery;
        asMatrix:boolean = false;
        name:string;
        onClick:Function;

      //  DEG_TO_RAD = Math.PI / 180;

     //   matrix:Matrix2D;
       // transformMatrix:Matrix2D;
        style:CSSStyleDeclaration;

        view:HTMLElement;
        $view:JQuery
        transform:string;
        origin:string


        ar:number[];

        constructor(view:HTMLElement,transform:string,origin:string,name:string){
            this.name=name;
            this.view = view;
            this.$view = $(view);
            this.transform = transform;
            this.origin = origin;
            this.style = window.getComputedStyle(view,null);

        }
        addClick(callBack:Function):void {
            //this.onClick = callBack;
            this.$view.click(callBack);
        }

        children:JQuery[]=[];
        addChild(el:JQuery){
            this.children.push(el)
            this.$view.append(el);
        }

        drawCenter():void{
            this.center = $('<div>').addClass('dot center').appendTo(this.$view);
        }



        setCenter(x:number,y:number):DisplaySimple{
            var p:Point = new Point;
            this.regX = x;
            this.regY = y;
            this.posX = - this.regX;
            this.posY = - this.regY;
            //this.posX+=dx;
           //console.log('this.posX: '+this.posX+'  this.posY: '+this.posY);
          // console.log('this.regX: '+this.regX+'  this.regY: '+this.regY);
            if(this.center)this.center.css({left:x,top:y});
            this.applyReg();
            this.apply();
            return this;
        }

        applyReg():DisplaySimple{
            this.view.style[this.origin]=this.regX+'px '+this.regY+'px ';
            this.apply();

            return this;
        }

        setAngle(ang:number):DisplaySimple{
            // this.rotation= ang*this.DEG_TO_RAD;
            this.angle=ang;
            return this
        }

        setScale(x:number):DisplaySimple{
            this.scaleX = x;
            return this;
        }

        setAS(a:number,s:number):void{
            this.scale=s;
            this.scaleX=s;
            this.scaleY=s;
            this.angle=a;
            this.apply();
        }

        move(x:number,y:number):void{
            this.posX=x;
            this.posY=y;
            this.apply();
        }

        apply():DisplaySimple{
            this.view.style[this.transform]= 'translate('+this.posX+'px,'+this.posY+'px) rotate('+this.angle+'deg) scale('+this.scale+') translateZ(0)';
            this.ar=null;
            return this;
        }

        private matrixToArray(str:string):string[] {
            return str.split('(')[1].split(')')[0].split(',');
        }
        private mCache:number[]
/*
        readMatrixVO():MatrixVO{
            var vo=new MatrixVO();
            var ar=this.readMatrixAr();
            vo.a = ar[0];
            vo.b = ar[1];
            vo.c = ar[2];
            vo.d = ar[3];
            vo.tx = ar[4];
            vo.ty = ar[5];
            return vo
        }
        */
        readMatrixAr():number[]{
           // var trans = this.style.getPropertyValue('-webkit-transform');
          //  console.log(trans);
            return this.matrixToArray(this.style.getPropertyValue('-webkit-transform')).map(Number);
            // return this.mCache
        }

        toGlobal(x:number, y:number):Point {
            var rx:number=this.regX;
            var ry:number=this.regY;
            x=x-rx;
            y=y-ry;
            if(!this.ar) this.ar = this.readMatrixAr();
            var ar:number[] = this.ar;
            var pt =  new Point();
            pt.x = (x * ar[0]) + (y * ar[2]) + ar[4]+rx;
            pt.y = (x * ar[1]) + (y * ar[3]) + ar[5]+ry;
            return pt;
        }

        toLocal(a:number,b:number):Point{
            var pt =  new Point();
           // var rx:number=this.regX;
           // var ry:number=this.regY;
            if(!this.ar) this.ar = this.readMatrixAr();
            var ar:number[] = this.ar;
            pt.x=(b*ar[2]-a*ar[3]+ar[4]*ar[3]-ar[5]*ar[2])/(ar[1]*ar[2]-ar[0]*ar[3]);//-this.regX;
            pt.y=(b-pt.x*ar[1]-ar[5])/ar[3];//-this.regY;


            return pt;
        }

      /*
        globalToLocal(x:number, y:number, pt?:Point) {
            return this.getConcatenatedMatrix().invert().transformPoint(x,y, pt);
        }
        localToGlobal(x:number, y:number, pt?:Point) {
            return this.getConcatenatedMatrix().transformPoint(x,y,pt);
        }
        localToGlobalMatr(x:number, y:number):Point{
            return this.matrix.transformPoint(x,y);
        }

        getConcatenatedMatrix = function():Matrix2D {
            var o:DisplaySimple = this;
            var mtx:Matrix2D = this.getMatrix();
            // while (o = o.parent) {
            // mtx.prependMatrix(o.getMatrix());
            //}
            return mtx;
        }
        /*
        getMatrix() {
            var o:DisplaySimple = this;
            var mtx:Matrix2D =  new Matrix2D();
            console.log(mtx.toString());
            var nm = mtx.appendTransform(o.posX, o.posY, o.scaleX, o.scaleY, o.angle, o.skewX, o.skewY, o.regX, o.regY);
            console.log(nm.toString());
            return nm
        }


         getMatrix(matrix:Matrix2D) {
         var o:DisplaySimple = this;
         var mtx:Matrix2D = matrix && matrix.identity() || new Matrix2D(1,0,0,1,0,0);
         return o.transformMatrix ?  mtx.copy(o.transformMatrix) : mtx.appendTransform(o.x, o.y, o.scaleX, o.scaleY, o.rotation, o.skewX, o.skewY, o.regX, o.regY);
         }
         */




    }

}