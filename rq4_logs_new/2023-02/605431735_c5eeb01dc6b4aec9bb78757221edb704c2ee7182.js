import Square from './square.js'

class Field{
    #width = 0;
    #height = 0;
    #squares = [];

    constructor(width, height, bombNum){
        this.#width = width;
        this.#height = height;
        const squaresNum = this.#width * this.#height;
        const bombIndexSet = new Set();
        while(bombIndexSet.size < bombNum){
            bombIndexSet.add(Math.floor(Math.random() * squaresNum));
        }
        console.log(...bombIndexSet);


        const calcNumOrBomb = i => {
            if(bombIndexSet.has(i)){
                return 'b';
            }
            const surroundIndex = [];
            const x = i % this.#width;
            if(x > 0){
                surroundIndex.push(i-1);//[x-1, y]
                surroundIndex.push(i-1-this.#width);//[x-1, y-1]
                surroundIndex.push(i-1+this.#width);//[x-1, y+1]
            }
            surroundIndex.push(i-this.#width);//[x, y-1]
            surroundIndex.push(i+this.#width);//[x, y+1]
            if(x !== this.#width - 1){
                surroundIndex.push(i+1);//[x+1, y]
                surroundIndex.push(i+1-this.#width);//[x+1, y-1]
                surroundIndex.push(i+1+this.#width);//[x+1, y+1]
            }
            return surroundIndex.filter(index => bombIndexSet.has(index)).length;
        }

        for(let i = 0; i < squaresNum; i++){
            this.#squares.push(new Square(calcNumOrBomb(i)));
        }
    }

    print(){
        for(let y = 0; y < this.#height; y++){
            let str = "";
            for(let x = 0; x < this.#width; x++){
                str += this.#squares[x + y * this.#width].numOrBomb;
            }
            console.log(str);
        }
    }
}

export default Field;