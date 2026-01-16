/// <reference path="gameobject.ts" />


class Catapult extends GameObject {
    
    private game: Game;
    private character: Character;
    
    constructor(g: Game, c: Character){
        super("catapult", document.getElementById("container"), 0, 480);
        
        this.game = g;
        this.character = c;
        
        this.div.addEventListener("click", this.onClick);
    }
    
    private onClick(){
        console.log(this.character);
        this.character.state = new Flying(this.character);
    }
    
    
}