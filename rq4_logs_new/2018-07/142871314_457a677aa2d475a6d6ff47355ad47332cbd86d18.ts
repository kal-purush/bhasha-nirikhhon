import { Elevator } from "./models/elevator";
import * as interfaces from './interfaces/interfaces';
import { ElevatorState } from './models/elevator-state';
import {minBy} from 'lodash';

class ElevatorAlligator {

    numFloors: number;
    numElevators: number;
    state: Map<number, ElevatorState>;
    //elevators: Array<Elevator>;

    constructor(numFloors: number, numElevators: number) {
        
        this.numFloors = numFloors;
        this.numElevators = numElevators;
        this.state = new Map();

        for (let i = 1; i <= this.numElevators; i++) {
            const elevatorState = new ElevatorState(this.numFloors);
            this.state.set(i, elevatorState);
        }
    }

    assignElevatorToRequest(requestedFloor: number, requestedDirection: string): interfaces.AssignElevatorResponse { 
        let response;

        const distancesOfIdleElevators = {}; // distances from requested floor

        for( let [elevatorId, elevatorState] of this.state ) {
            
            if(this.fullfillsPriority1(elevatorState, requestedFloor)) {
                response = { elevatorId };
                break;
            }

            if(this.fullfillsPriority2(elevatorState, requestedFloor, requestedDirection)) {
                response = { elevatorId };
                break;
            }

            if(elevatorState.direction === null) { // elevator is idle
                let distanceFromRequestedFloor = Math.abs(elevatorState.currentFloor - requestedFloor);
                distancesOfIdleElevators[elevatorId] = distanceFromRequestedFloor;
            }
        }

        if(!response && (Object.keys(distancesOfIdleElevators).length) > 0 ) {
            var elevatorId = minBy(Object.keys(distancesOfIdleElevators), d => distancesOfIdleElevators[d] );
            response = { elevatorId: parseInt(elevatorId) }
        }

        if(!response) response = {
            message: 'could not assign elevator' //TO DO: use a queue and reject this msg, then retry until assigned
        }

        return response;
    }

    private fullfillsPriority1(elevatorState: ElevatorState, requestedFloor: number): boolean {
        // Elevator is stopped at the requested floor
        return (elevatorState.currentFloor === requestedFloor && elevatorState.direction === null); 
    }
    
    private fullfillsPriority2(elevatorState: ElevatorState, requestedFloor: number, requestedDirection: string): boolean {
        // Elevator will pass by requested floor
        // Assuming an elevator needs at least 2 floors to make a complete break

        let directionsMatch = elevatorState.direction === requestedDirection;
        let willPassByOnWayUp = (requestedFloor - elevatorState.currentFloor) > 2 && requestedDirection === 'U';
        let willPassByOnWayDown = (elevatorState.currentFloor - requestedFloor) > 2 && requestedDirection === 'D';

        return ((willPassByOnWayDown || willPassByOnWayUp) && directionsMatch);
    }

}

const numFloors = parseInt(process.argv[2]);
const numElevators = parseInt(process.argv[3]);

export const elevatorAlligator = new ElevatorAlligator(numFloors, numElevators);
console.log(elevatorAlligator.state);