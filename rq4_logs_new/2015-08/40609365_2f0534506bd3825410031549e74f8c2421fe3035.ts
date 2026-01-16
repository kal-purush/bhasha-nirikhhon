/**
 * Created by qlm on 2015/7/13.
 */
class Animal{
    name:string;

    constructor(theName:string){
        this.name = theName;
    }

    move(meters:number = 0){
        alert(this.name + " moved" + meters + "m.");
    }
}

class Snake extends Animal{
    constructor(name:string){super(name);}

    move(meters = 5){
        alert("Slithering...");
        super.move(meters);
    }
}

class Horse extends Animal{
    constructor(name:string){super(name);}

    move(meters = 45){
        alert('Galloping...');
        super.move(meters);
    }

    private getName(){
        return this.name;
    }
}

interface  StringValidator{
    isAcceptable(s:string) : boolean;
}

var lettersRegexp = /^[A-Za-z]+$/;

class LettersOnlyValidator implements StringValidator{
    isAcceptable(s:string){
        return lettersRegexp.test(s);
    }
}

var obj = {
    name : "lucy",
    getName:function() {
        return () => {
            return {
                n : this.name
            };
        }
    }
};
var func = obj.getName();
alert(func()["n"]);