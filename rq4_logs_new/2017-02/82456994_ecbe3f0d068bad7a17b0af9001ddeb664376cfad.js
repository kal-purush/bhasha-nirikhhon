/**
 * Created by DELL on 20.02.2017.
 */

class Animal {

    constructor(type, name) {
        this.name = name;
        this.type = type;
    }

    get getName() {
        return this.name;
    }

    get getPhrase() {
        return `\n - I am super ${this.type}! Say ${this.name}`;
    }

    action() {
        return "doing something";
    }
}

//-----------------------------------------------------------------------------

class Rabbit extends Animal {

    constructor(type, name) {
        super(type, name);
    }

    get getName() {
        return super.getName;
    }

    get getPhrase() {
        return super.getPhrase;
    }

    action() {
        return "jump!";
    }
}

//-----------------------------------------------------------------------------

class Wolf extends Animal {

    constructor(type, name) {
        super(type, name);
    }

    get getName() {
        return super.getName;
    }

    get getPhrase() {
        return super.getPhrase;
    }

    action() {
        return "start eat rabbits.";
    }

}

//-----------------------------------------------------------------------------

class BigRabbit extends Rabbit{
    constructor(type,name){
        super(type,name);
    }

    get getName() {
        return super.getName;
    }

    get getPhrase() {
        return super.getPhrase;
    }

    action() {
        return "start fight with wolf.";
    }

}
//-----------------------------------------------------------------------------

let rabbit = new Rabbit("rabbit", "Bob");
let wolf = new Wolf("wolf", "John");
let bigRabbit = new BigRabbit("big rabbit", "Mike");

console.log(rabbit.getPhrase + " and " +  rabbit.action());
console.log(wolf.getPhrase + " and " +  wolf.action());
console.log(bigRabbit.getPhrase + " and " +  bigRabbit.action());