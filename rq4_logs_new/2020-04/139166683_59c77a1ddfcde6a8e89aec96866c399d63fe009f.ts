import { QuackBehavior } from './QuackBehavior';

class Squawk implements QuackBehavior {
    quack(): void {
        console.log('It is squawking');
    }
}

export { Squawk as Squawk };