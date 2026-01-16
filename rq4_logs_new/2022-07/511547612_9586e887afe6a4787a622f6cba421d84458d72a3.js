const tank = {
    value : 1, // player value 1 
    display : "⁂",
    width : 4,
    height : 4,
    x : Math.floor(board.width/2), // 중간에서 시작!
    y : Math.floor(board.height/2),
    hp : 1,
}

const enemy = {
    value : 3,
    display : "◈",
    width : 5,
    height : 5,
    x : 10,
    y : 10,
    hp : 5,
}

// 물체의 범위를 정의!
function objRange(obj){
    const xRange = [obj.x-Math.floor(obj.width/2), obj.x+Math.floor(obj.width/2)];
    const yRange = [obj.y-Math.floor(obj.height/2), obj.y+Math.floor(obj.height/2)];
    // [0][0] tank Left / [0][1] tank Right / [1][0] tank Up / [1][1] tank Down
    return [xRange, yRange];
}

// 잔상제거를 위한 함수, 작동원리는 display 와 동일
function removeObj(obj){
    const range = objRange(obj);
    const tr = document.querySelectorAll('tr');
    for(let i=range[1][0]; i<range[1][1]; i++){
        const td = tr[i].querySelectorAll('td');
        for(let j=range[0][0]; j<range[0][1]; j++){
            td[j].innerText = SPACE_VALUE;
        }
    }

}
// object display! 
function displayObj(obj){
    const range = objRange(obj);
    const tr = document.querySelectorAll('tr');
    for(let i=range[1][0]; i<range[1][1]; i++){
        const td = tr[i].querySelectorAll('td');
        for(let j=range[0][0]; j<range[0][1]; j++){
            td[j].innerText = obj.display;
        }
    }
    
}

// 물체의 움직임!
function objMove(command, obj){
    const range = objRange(obj);
    removeObj(obj);
    if (command === 38) { //Up
        if (range[1][0] > 1 ){
            obj.y -= 1;
        }
    } else if (command === 39) {// right
        if (range[0][1] < board.width-1){
            obj.x += 1;
        }
    } else if (command === 40) {// down
        if (range[1][1] < board.height-1){
            obj.y += 1;
        }
    } else if (command === 37) {// left
        if (range[0][0] > 1){
            obj.x -= 1;
        }
    }
}
function randomMove(){
    // eneymove! 
    return Math.floor((Math.random()*10) % 4) + 37
}
document.addEventListener('keydown', function(event){
    objMove(event.keyCode, tank);
    objMove(randomMove(), enemy);
});
/*
function enemyMove(command){
    const range = objRange(enemy);
    removeObj(enemy);
    if (command === 0){ // Up
        if (range[1][0] > 1) {
            enemy.y -= 1;
        }
    } else if (command === 1) { // right
        if (range[0][1] < board.width-1){
            enemy.x += 1;
        }
    } else if (command === 2) {
        if (range[1][1] < board.width-1 )
    }
}*/