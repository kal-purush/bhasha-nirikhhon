export default class Team {
  constructor() {  
  this.hero = [
    {name: 'Лучник', health: 50, level: 1, type: 'Bowman', attack: 25, defence: 25},
    {name: 'Мечник', health: 50, level: 1, type: 'Swordsman', attack: 40, defence: 10},
    {name: 'Безсмертный', health: 50, level: 1, type: 'Undead', attack: 10, defence: 40},
    {name: 'Маг', health: 50, level: 1, type: 'Magician', attack: 25, defence: 25}
  ]
}

[Symbol.iterator](){
  let hero = this.hero;    
  let current = 0;    
  return {next(){        
      if(current < hero.length){
          let done = false;
              let value = hero[current];
          console.log(value);
          current++;
              return {
              done, value
          }} else{return {done: true}}
      }
  }
}
}