import { Observer } from 'rxjs';

export class ObserverExample implements Observer<number> {
    next(data: number): void {
        console.log('observerExample ' + data);
    }
    error(err: any): void {
        console.log(err);
    }

    complete(): void {
        console.log('complete done');
    }
    
}