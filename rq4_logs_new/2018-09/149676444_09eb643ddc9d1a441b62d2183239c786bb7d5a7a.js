this.player = function (){
    this.name = name;
    this.position = position;
    this.offense = offense;
    this.defense = defense;
};

this.goodgame = function(){
    if(Math.floor(Math.random() * 2) ===0){
        this.offense++;
        console.log(this.name + "'s offense has gone up!\n---------");
    }
    else{
        this.defense++;
        console.log(this.name + "'s defense has gone up!\n----------");
    }
};

this.badgame = function(){
    if(Math.floor(Math.random() * 2) ===0){
        this.offense --;
        console.log(this.name + "'s offense has gone down!\n---------");
    }
    else{
        this.defense--;
        console.log(this.name + "'s defense has gone down!\n----------");
    }
};

this.printStates = function(){
    console.log("Name: " + this.name + "\nPosition: " + this.position + "\nOffense: " + this.offense + "\nDefense: " + this.defense + "\n---------")
}
