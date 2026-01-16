class Ninja{
    constructor(name, health){
        this.name = name;
        this.health = 100; 
        this.speed = 3; 
        this.strength= 3;
    }

    sayName() {
        console.log(this.name);
    }

    showStats(){
        console.log(this.name, this.strength, this.speed, this.health);
    }

    drinkSake(){
        this.health += 10;
    }


}

const ninja1 = new Ninja("Hyabusa");
ninja1.drinkSake();
ninja1.sayName();
ninja1.showStats();