import {range, random} from "lodash";

import Result from "../model/Result";


export default class ResultGenerator {

    static generateNumber(pastNumbers : number[], min : number, max : number) : number {
        const availableNumbers = range(min, max + 1).filter(n => pastNumbers.indexOf(n) === -1);
        if(availableNumbers.length === 0) {
            return random(min, max);
        }
        const index = random(0, availableNumbers.length - 1);
        return availableNumbers[index];
    }

    static generate(pastResults : Result[]) : Result {
        const num1 = ResultGenerator.generateNumber(pastResults.map(r => r.num1), 1, 50);
        const num2 = ResultGenerator.generateNumber(pastResults.map(r => r.num2), 1, 50);
        const num3 = ResultGenerator.generateNumber(pastResults.map(r => r.num3), 1, 50);
        const num4 = ResultGenerator.generateNumber(pastResults.map(r => r.num4), 1, 50);
        const num5 = ResultGenerator.generateNumber(pastResults.map(r => r.num5), 1, 50);

        const star1 = ResultGenerator.generateNumber(pastResults.map(r => r.star1), 1, 12);
        const star2 = ResultGenerator.generateNumber(pastResults.map(r => r.star2), 1, 12);

        return new Result(num1, num2, num3, num4, num5, star1, star2);
    }
}