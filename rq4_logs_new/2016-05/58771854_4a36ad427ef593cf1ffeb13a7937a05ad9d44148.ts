// // interface IPoint {
// //     getPoint():{x:number,y:number};
// // }
// abstract class ACPoint {
//     getPoint() {
//         return {x: 1, y: 2};
//     }
// }
//
// class Point extends ACPoint {
//     private x:number;
//     private y:number;
//
//     constructor(x:number, y:number) {
//         super();
//         this.x = x;
//         this.y = y;
//     }
//
//     // getPoint():{x:number,y:number} {
//     //     return {x: this.x, y: this.y};
//     // }
// }

// declare var MyPoint:{x:number,y:number};


// interface SomePoint {
//     x:number;
//     y:number;
// }
// declare var MyPoint:SomePoint;

/// <reference path="../typings/modules/lodash/index.d.ts" />


namespace Shipping {
    export interface Ship {
        name:string;
        port:string;
    }

    export class Ferry {
        name:string;
    }
}

namespace Docking {
    import Ship=Shipping.Ship;
}

let b:Shipping.Ship;
let a = new Shipping.Ferry();
