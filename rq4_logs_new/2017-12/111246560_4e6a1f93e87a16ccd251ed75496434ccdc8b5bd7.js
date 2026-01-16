const grav = 4;
const tileSize = 32;
const mapSize = 512;
const app = document.getElementById('app');
let speed = 0;

const level1 = [[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,1,1,1,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0],
                [0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,0],
                [0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1],
                [1,1,1,1,1,0,0,1,1,1,1,1,1,1,1,1],
                [1,1,1,1,1,0,0,1,1,1,1,1,1,1,1,1],
                [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
                ]

let currLevLog = [];



console.log('foo');

function makeLevel(lev){
    currLevLog = [];
    for(let i = 0; i < lev.length; i++){
        for(let j = 0; j < lev[i].length; j++){
            const levObj = {};
            if(lev[i][j] == 0){
                levObj.type = 'sky';
                levObj.xLeft = j * tileSize;
                levObj.xRight = ((j+1) * tileSize)-1;
                levObj.yTop = i * tileSize;
                levObj.yBottom = ((i+1) * tileSize)-1;
                /*
                const skyTile = document.createElement('div');
                skyTile.classList += 'skyTile';
                app.appendChild(skyTile);
                */
            }
            else if(lev[i][j] == 1){
                levObj.type = 'grass';
            }
            currLevLog.push(levObj);
        }
        
    }
}


(function init(){
    makeLevel(level1);
    console.log(currLevLog)
})()