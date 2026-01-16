var b:boolean = false;
var n:number = 3;
var s:string = 'Chris';

var list:number[] = [2, 3, 4];
var list2:Array<string> = ['s', 'sddd', 'dddd'];

enum Color {Red = 1, Green, Blue}
var c:Color = Color.Blue;
var list3:Array<Color> = [Color.Red, Color.Green];

function f():void {
    console.log('something');
}

function f1(object:{label: string}) {
    console.log(object.label);
}
//f1({label: '2', size: 10});

interface LabeledObject {
    label: string;
}
function f2(object:LabeledObject) {
    f1(object);
}
//f2({label: '22', size: 10});

enum Size {XS, S, L, XL, XXL}
interface Config {
    size?: Size;
    color?: Color;
}
class TShirt {
    constructor(public config: Config){}
}
function createTShirt(config: Config): TShirt {
    var tShirtConfig = {color: Color.Red, size: Size.XS};
    if(config.color) {
        tShirtConfig.color = config.color;
    }
    if(config.size) {
        tShirtConfig.size = config.size;
    }

    return new TShirt(tShirtConfig);
}
console.log(createTShirt({color: Color.Green}));
console.log(createTShirt({size: Size.XXL}));
console.log(createTShirt({}));