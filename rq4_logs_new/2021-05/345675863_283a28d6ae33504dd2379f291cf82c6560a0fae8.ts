import Simplified, { possibleStates } from "../../../helpers/responseInterfaces/Simplified";


const setTaskState = (task: Simplified ,newState: possibleStates): Simplified => {
    return {...task, state: newState};
}

const setNextState = (task: Simplified): Simplified => {
    switch(task.state) {
        case possibleStates.NOT_STARTED: return setTaskState(task, possibleStates.IN_PROGRESS);
        case possibleStates.IN_PROGRESS: return setTaskState(task, possibleStates.DONE);
        default: return task;
    }
}

const setPrevState = (task: Simplified): Simplified => {
    switch(task.state) {
        case possibleStates.DONE: return setTaskState(task, possibleStates.IN_PROGRESS);
        case possibleStates.IN_PROGRESS: return setTaskState(task, possibleStates.NOT_STARTED);
        default: return task;
    }
}

export {setNextState, setPrevState};