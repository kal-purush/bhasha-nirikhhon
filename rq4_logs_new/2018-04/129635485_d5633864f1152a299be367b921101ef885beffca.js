class Alien {
  constructor(){
    this.setAlien();
    setInterval(this.alienMove(), 3000);
  }

  setAlien(){
    let col = Math.floor(Math.random() * Math.floor(100));
    let row = Math.floor(Math.random() * Math.floor(60));

    let alien = document.getElementById(`row-${row}-col-${col}`);

    while (alien.getAttribute('class') !== null){
      col = Math.floor(Math.random() * Math.floor(100));
      row = Math.floor(Math.random() * Math.floor(60));
    }
    alien.setAttribute("alien", "alien");
    return alien;
  }

  getColAndRowFromLi(player) {
    let col = Number(player[0].id.match(/\d+/g)[1]);
    let row = Number(player[0].id.match(/\d+/g)[0]);
    return [row, col];
  }

  moves(){
    let alien = document.querySelectorAll('[alien="alien"]');
    let [row, col] = this.getColAndRowFromLi(alien);
    //8 neighbours
    let neighbours = [];

    if(document.getElementById(`row-${row}-col-${col+1}`)) neighbours.push(document.getElementById(`row-${row}-col-${col+1}`));
    if(document.getElementById(`row-${row}-col-${col-1}`)) neighbours.push(document.getElementById(`row-${row}-col-${col-1}`));
    if(document.getElementById(`row-${row+1}-col-${col}`)) neighbours.push(document.getElementById(`row-${row+1}-col-${col}`));
    if(document.getElementById(`row-${row-1}-col-${col}`)) neighbours.push(document.getElementById(`row-${row-1}-col-${col}`));
    if(document.getElementById(`row-${row-1}-col-${col-1}`)) neighbours.push(document.getElementById(`row-${row-1}-col-${col-1}`));
    if(document.getElementById(`row-${row-1}-col-${col+1}`)) neighbours.push(document.getElementById(`row-${row-1}-col-${col+1}`));
    if(document.getElementById(`row-${row+1}-col-${col-1}`)) neighbours.push(document.getElementById(`row-${row+1}-col-${col-1}`));
    if(document.getElementById(`row-${row+1}-col-${col+1}`)) neighbours.push(document.getElementById(`row-${row+1}-col-${col+1}`));

    return neighbours;
  }


  alienMove() {
    let arrOfMoves = this.moves();
    let alien = document.querySelectorAll('[alien="alien"]')[0];
    let move = arrOfMoves[Math.floor(Math.random() * Math.floor(arrOfMoves.length))];
    while (move.getAttribute('class')){
       move =  document.getElementById(arrOfMoves[Math.floor(Math.random() * Math.floor(arrOfMoves.length))]);
    }

    alien.removeAttribute('alien');
    move.setAttribute("alien", "alien");
    this.alien = move;
    return this.alien;
  }



}

export default Alien;