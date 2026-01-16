import 'reflect-metadata';

function example() {
    return function(targetClass) {
        Reflect.getMetadata('design:paramtypes', targetClass);
        return targetClass;
    }
}

@example()
class Person {
    constructor(name: string) {}
};


new Person('Bill');