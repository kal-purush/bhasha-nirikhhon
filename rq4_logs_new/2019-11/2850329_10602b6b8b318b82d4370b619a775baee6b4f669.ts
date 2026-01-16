import { Observable } from '@apestaartje/observable/dist/observable/Observable';
import { SafeObserver } from '@apestaartje/observable/dist/observer/SafeObserver';
import { Subscription } from '@apestaartje/observable/dist/observable/Subscription';

import { Action } from '@tetris/control/Action';
import { Control } from '@tetris/control/Control';

const CLOCKWISE: string = 's';
const COUNTER_CLOCKWISE: string = 'a';
const DOWN: string = 'ArrowDown';
const LEFT: string = 'ArrowLeft';
const RIGHT: string = 'ArrowRight';

export function keyboard(): Control {
    return new Observable<Action>((observer: SafeObserver<Action>): Subscription => {
        function handle(event: KeyboardEvent): void {
            switch (event.key) {
                case LEFT:
                    observer.next(Action.Left);
                    break;
                case RIGHT:
                    observer.next(Action.Right);
                    break;
                case CLOCKWISE:
                    observer.next(Action.ClockWise);
                    break;
                case COUNTER_CLOCKWISE:
                    observer.next(Action.CounterClockWise);
                    break;
                case DOWN:
                    observer.next(Action.Down);
                    break;
            }
        }

        window.addEventListener('keydown', handle);

        return {
            unsubscribe(): void {
                window.removeEventListener('keydown', handle);
            },
        };
    });
}