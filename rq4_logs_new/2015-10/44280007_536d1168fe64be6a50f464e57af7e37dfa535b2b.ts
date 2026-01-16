module SqGame{
    export interface ISquaresDirectiveScope extends ng.IScope {
        initMovesQty: number;
        squareColors: Array<string>;
        level:number;
        timesClicked:number;
        highlighted:number;
        sqNumbers:Array<number>;
        newGame(): void;
        movesQty(): number;
        checkSequence(clickedSquare:number):void;
    }
}