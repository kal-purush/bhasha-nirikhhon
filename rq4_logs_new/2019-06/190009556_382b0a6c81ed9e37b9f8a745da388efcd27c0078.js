const add = function (){
    return arguments[0] + arguments[1];
}

console.log(add(11, 22, 33, 44));

const car = {
    color: 'Red',
    getSummary: function () {
        return `the car is ${this.color}`;
    }
}

console.log(car.getSummary());