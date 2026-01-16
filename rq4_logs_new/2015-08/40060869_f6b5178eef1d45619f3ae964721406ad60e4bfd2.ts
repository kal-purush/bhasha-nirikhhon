module galaxySim {
    interface drawer {
        circle(center:coords2d,radius:number,colour:string):void;
        line(endpoints:coords2d[],weight:number,colour:string);
        clear():void;
    }

    interface drawable {
        draw(out:drawer):void;
    }

    interface steppable {
        step(time:number):void;
    }

    interface physObj {
        accelerate(force:vector2d,time:number):void;
        getLoc():coords2d;
        getVelocity():vector2d;
        getMass():number;
    }

    export class vector2d {
        constructor(public x:number,public y:number) {}
        public static sum(a:vector2d,b:vector2d):vector2d {return new vector2d(a.x+b.x,a.y+b.y);}
        public static times(m:number,v:vector2d):vector2d {return new vector2d(v.x*m,v.y*m);}
        public static div(v:vector2d,d:number):vector2d {return new vector2d(v.x/d,v.y/d);}
        public static mag(v:vector2d) { return Math.sqrt(v.x * v.x + v.y * v.y); }
        public static norm(v:vector2d) {
            var mag = vector2d.mag(v);
            var div = (mag === 0) ? Infinity : 1.0 / mag;
            return vector2d.times(div, v);
        }
    }

    export class coords2d extends vector2d {
        public static difference(a:coords2d,b:coords2d):vector2d {
            return new vector2d(b.x-a.x,b.y-a.y);
        }
    }

    export class SVGDrawer implements drawer {
       constructor(private target:HTMLElement) {}

       clear() {
           while(this.target.firstChild) this.target.removeChild(this.target.firstChild);
       }

       circle(center:coords2d,radius:number,colour:string):void {
           var elm = document.createElementNS('http://www.w3.org/2000/svg','circle');
           elm.setAttributeNS(null, 'cx', center.x.toString());
           elm.setAttributeNS(null, 'cy', center.y.toString());
           elm.setAttributeNS(null, 'r', radius.toString());
           elm.setAttributeNS(null, 'stroke-width', '0');
           elm.setAttributeNS(null, 'fill', colour);
           this.target.appendChild(elm);
       }

       line(endpoints:coords2d[],weight:number,colour:string):void {
           var elm = document.createElementNS('http://www.w3.org/2000/svg','line');
           elm.setAttributeNS(null, 'x1', endpoints[0].x.toString());
           elm.setAttributeNS(null, 'y1', endpoints[0].y.toString());
           elm.setAttributeNS(null, 'x2', endpoints[1].x.toString());
           elm.setAttributeNS(null, 'y2', endpoints[1].y.toString());
           elm.setAttributeNS(null, 'stroke-width', weight.toString());
           elm.setAttributeNS(null, 'stroke', colour);
           this.target.appendChild(elm);
       }
   }

   export class scaledDrawer implements drawer {
      constructor(private parent:drawer,private scale:number) {}

      circle(center:coords2d,radius:number,colour:string):void {
          this.parent.circle(vector2d.div(center,this.scale),radius/this.scale,colour);
      }

      line(endpoints:coords2d[],weight:number,colour:string):void {
          this.parent.line([vector2d.div(endpoints[0],this.scale),vector2d.div(endpoints[1],this.scale)],weight,colour);
      }

      clear() {this.parent.clear();}
  }



    export class pointObj implements drawable,physObj {
        getLoc():coords2d {
            return new coords2d(0,0);
        }

        draw(out:drawer):void {
            out.circle(this.getLoc(),this.getRadius(),'black');
        }

        accelerate(force:vector2d,time:number) {}
        getVelocity():vector2d {return new vector2d(0,0);}
        getMass() {return 0;}
        getRadius() {return 5;};
    }

    export class simplePoint extends pointObj {
        constructor(private loc:coords2d) {
            super();
        }

        getLoc() {
            return this.loc;
        }

        move(loc:coords2d) {
            this.loc = loc;
        }
    }

    export class movingPoint extends pointObj implements steppable {
        constructor(private loc:coords2d,public velocity:vector2d) {
            super();
        }

        step(time:number) {
            this.loc = vector2d.sum(this.loc,vector2d.times(time,this.velocity));
        }

        getLoc():coords2d {
            return this.loc;
        }

        getVelocity():vector2d {return this.velocity;}
    }

    export class weightedPoint extends movingPoint implements physObj {
        constructor(loc:coords2d,velocity:vector2d,private mass:number) {
            super(loc,velocity);
        }

        accelerate(force:vector2d,time:number) {
            this.velocity = vector2d.sum(this.velocity,vector2d.times(time,galaxySim.vector2d.div(force,this.mass)));
        }

        getMass() {
            return this.mass;
        }
    }

    export class planet extends weightedPoint {
        constructor(loc:coords2d,velocity:vector2d,mass:number,private raduis:number) {
            super(loc,velocity,mass);
        }

        getRadius() {return this.raduis;}
    }

    export class controller implements steppable,drawable {
        private steppers:steppable[] = [];
        private drawers:drawable[] = [];

        step(time:number) {
            for(var i = 0;i < this.steppers.length;i++) this.steppers[i].step(time);
        }

        draw(out:drawer) {
            for(var i = 0;i < this.drawers.length;i++) this.drawers[i].draw(out);
        }

        addStepper(s:steppable) {this.steppers.push(s);}
        addDrawer(d:drawable)   {this.drawers.push(d);}

        addStepperList(s:steppable[]) {for(var i = 0;i < s.length;i++) this.steppers.push(s[i]);}
        addDrawerList(d:drawable[])   {for(var i = 0;i < d.length;i++) this.drawers.push(d[i]);}
    }

    export class gravity implements steppable {
        private objs:physObj[] = [];
        constructor(private gravitationalConstant:number) {}

        gravitate(s:physObj) {this.objs.push(s);}
        gravitateList(s:physObj[]) {for(var i = 0;i < s.length;i++) this.gravitate(s[i]);}

        step(time:number) {
            for(var a = 0;a < this.objs.length;a++)
                for(var b = a+1;b < this.objs.length;b++) {
                    var massProduct = this.objs[a].getMass() * this.objs[b].getMass();
                    var diff = coords2d.difference(this.objs[a].getLoc(),this.objs[b].getLoc());
                    var r = vector2d.mag(diff);
                    var norm = coords2d.norm(diff);
                    var f = this.gravitationalConstant*(massProduct/(r*r));
                    this.objs[a].accelerate(vector2d.times(f,norm),time);
                    this.objs[b].accelerate(vector2d.times(-f,norm),time);
                }
        }
    }
}