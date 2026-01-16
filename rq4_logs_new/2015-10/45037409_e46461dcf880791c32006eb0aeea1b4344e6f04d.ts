import {Sum} from "./sum";

export default class Calc {

    public Display : number;

    constructor(display : number = 0) {
        this.Display = display;
    }

    sum(num : number) : number {

        var sum = new Sum();

        this.Display += sum.result(num);
        return this.Display;
    }

}