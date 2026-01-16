interface IElevator {
    // Queue the elevator to go to specified floor number. If you specify true as second argument, the elevator will go to that floor directly, and then go to any other queued floors.
    goToFloor(floor: number): void;
    goToFloor(floor: number, directly: boolean): void;
    // Clear the destination queue and stop the elevator if it is moving. Note that you normally don't need to stop elevators - it is intended for advanced solutions with in-transit rescheduling logic.
    stop(): void;
    // Gets the floor number that the elevator currently is on.
    currentFloor(): number;
    // Gets or sets the going up indicator, which will affect passenger behaviour when stopping at floors.
    goingUpIndicator(): boolean;
    goingUpIndicator(enabled: boolean);
    // Gets or sets the going down indicator, which will affect passenger behaviour when stopping at floors.
    goingDownIndicator(): boolean;
    goingDownIndicator(enabled: boolean);
    // Gets the load factor of the elevator. 0 means empty, 1 means full. Varies with passenger weights, which vary - not an exact measure.
    loadFactor(): number;
    // The current destination queue, meaning the floors the elevator is scheduled to go to. Can be modified and emptied if desired. Note that you need to call checkDestinationQueue() for the change to take effect immediately.
    destinationQueue: number[];
    // Checks the destination queue for any new destinations to go to. Note that you only need to call this if you modify the destination queue explicitly.
    checkDestinationQueue(): void;
    // Event Handler
    on(event: string, cback: Function): void;
}
interface IFloor {
    // Gets the floor number of the floor object.
    floorNum(): number;
    // Event Handler
    on(event: string, cback: Function): void;
}

// Dummy IEFE due to reasons. Delete the first (
(() => { })();

module Elevation {
    export enum Direction {
        Up,
        Resting,
        Down,
    }
    class Elevator {
        private moving: boolean;
        getDirection(): Direction {
            if (!this.moving)
                return Direction.Resting;
            // TODO
            return Direction.Resting;
        }
    }

    export class ElevatorControl {
        init(elevators: IElevator[], floors: IFloor[]) {
            var nfloors = floors.length;
            var mfloor = Math.floor(nfloors / 2);
            var elevator = elevators[0]; // Let's use the first elevator
            elevator.on("idle", () => {
                elevator.goToFloor(mfloor);
            });

            elevator.on("floor_button_pressed", (num: number) => {
                if (elevator.destinationQueue.indexOf(num) === -1)
                    elevator.goToFloor(num);
            });
            floors.forEach((floor, i) => {
                var foo = () => { elevator.goToFloor(i); };
                floor.on("up_button_pressed", foo);
                floor.on("down_button_pressed", foo);
            });
        }
        update(dt: number, elevators: IElevator[], floors: IFloor[]) {
            // We normally don't need to do anything here
        }
    }
}

var test: Elevation.ElevatorControl;
test = new Elevation.ElevatorControl();
// Delete the last )