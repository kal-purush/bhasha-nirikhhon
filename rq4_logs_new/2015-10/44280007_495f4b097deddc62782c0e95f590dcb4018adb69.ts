class SquaresDirective{

    templateUrl: string;
    link: any;

    constructor($timeout:any){
        this.templateUrl = 'sqGame/directives/squares.html';

        SquaresDirective.prototype.link = (scope:any, elem:any) =>{
            let level = 1,
                highlighted = 0,
                initMovesQty = 3,
                movesQty = initMovesQty + level,
                sqNumbers: Array<number>,
                timesClicked = 0;

            scope.newGame = ()=>{
                console.log(sqNumbers);
                runSequence();
            }
            scope.squareColors = ['red', 'blue', 'green', 'yellow'];
            scope.checkSequence = (i:number)=>{
                if(sqNumbers[timesClicked] !== i){
                    alert('błąd!');
                } else{
                    timesClicked++;
                    if(timesClicked === movesQty){
                        timesClicked = 0;
                        alert('Wygrałeś!');
                    }
                }
            }
            let squaresQty = scope.squareColors.length;

            function randomNumbers(qty:number, max:number){
                let numbers:any[] = [];
                while(qty){
                    numbers.push(Math.floor(Math.random() * max));
                    qty--;
                }
                return numbers;
            }

            function runSequence(){
                sqNumbers = randomNumbers(movesQty, squaresQty);
                highlightSquare();

                function highlightSquare(){
                    $(`.square:eq(${sqNumbers[highlighted]})`, elem).css({'opacity': 1});
                    $timeout(()=>{
                        clearHighlights();
                    }, 500);
                }

                function clearHighlights(){
                    $(`.square`, elem).css({'opacity': 0.3});
                    $timeout(()=>{
                        highlighted++;
                        if(movesQty === highlighted){
                            highlighted = 0;
                            return;
                        }
                        highlightSquare();
                    }, 250);
                }
            }
        }
    }

    public static Factory() {
        var directive = ['$timeout', ($timeout:any) => {
            return new SquaresDirective($timeout);
        }];
        return directive;
    }
}

angular.module('app').directive('squares', SquaresDirective.Factory());