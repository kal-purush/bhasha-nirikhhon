import { Point } from "./point";


export class Triangle {
    constructor(private vertex1:Point, private vertex2:Point, private vertex3:Point) {};

    calculateLengthSides():number[] {
        const ladoUno = this.vertex1.calculateDistance(this.vertex2);
        const ladoDos = this.vertex2.calculateDistance(this.vertex3);
        const ladoTres = this.vertex3.calculateDistance(this.vertex1);
        return [ladoUno, ladoDos, ladoTres];
    };
};