/// <reference path="../common/Assert.ts"/>
/// <reference path="../common/Point2.ts"/>
/// <reference path="../common/StateMachine.ts"/>
/// <reference path="../display/Display.ts"/>
/// <reference path="../logic/Circle.ts"/>

module Cmd {
    export class Cmd {
        display: Display.Display
        
        constructor() {
            this.display = new Display.Display()
        }

        Circle(p: Point2, radius: number): LogicCircle.Circle {
            var p = new Point2(50, 50)
            var lc = new LogicCircle.Circle(p, 50)
            var dc = this.display.Circle()
            dc.setCenter(p);
            dc.setRadius(lc.radius);
            return lc;
        }
    }
}