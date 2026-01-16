
const DataAPI = require('../api/simulation-data').default;
var api = new DataAPI();

// const ProcessDataWorker = require('worker!../workers/processData');
const TestWorker = require('worker!../workers/processData');

export const Types = {
    SET_SIMULATION: 'SELECT_SIMULATION',
    SET_SIMULATION_VAR: 'SET_SIMULATION_VAR',

    PROCESS_DATA_START: 'PROCESSING_DATA_START',
    PROCESS_DATA_FINISH: 'PROCESSING_DATA_FINISH',
    PROCESS_DATA_ERROR: 'PROCESSING_DATA_ERROR',

    SIMULATION_REQUEST: 'SIMULATION_REQUEST',
    SIMULATION_REQUEST_SUCCESS: 'SIMULATION_REQUEST_SUCCESS',
    SIMULATION_REQUEST_FAILURE: 'SIMULATION_REQUEST_FAILURE',
    SIMULATION_FETCH: 'SIMULATION_FETCH',
};

//
// Direct User Actions
//

export function setSimulation(simulationName) {
    return function(dispatch, getState) {
        return Promise.all([
            dispatch({
                type: Types.SET_SIMULATION,
                name: simulationName
            }),
            dispatch(fetchSimulation(simulationName))
        ]).then(function() {
            var state = getState();
            var varNames = state.simulationsByName[state.selection.name].data.varNames;
            return dispatch(setSimulationVariable(varNames[0]));
        });
     };
};

export function setSimulationVariable(variableName) {
    return {
        type: Types.SET_SIMULATION_VAR,
        name: variableName
    };
};

//
// App Status Actions
//

export function fetchSimulation(simulationName) {
    return function(dispatch) {
        dispatch(requestSimulation(simulationName));
        return api.fetchSimulation(simulationName)
            .then(function(data) {
                dispatch(simulationRequestSuccess(simulationName, data));
            })
            .catch(function(err) {
                dispatch(simulationRequestFailure(simulationName, err));
            });
    }
};

export function requestSimulation(simulationName) {
    return {
        type: Types.SIMULATION_REQUEST,
        name: simulationName
    };
};

export function simulationRequestSuccess(simulationName, data) {
    return {
        type: Types.SIMULATION_REQUEST_SUCCESS,
        name: simulationName,
        data: data
    };
};

export function simulationRequestFailure(simulationName, err) {
    return {
        type: Types.SIMULATION_REQUEST_FAILURE,
        name: simulationName,
        err: err
    };
};

export function processData(simulationName) {
    return function(dispatch, getState) {
        var state = getState();
        var simData = state.simulationsByName[simulationName];

        dispatch({
            type: Types.PROCESS_DATA_START,
            name: simulationName
        });

        if (!simData) {
            return dispatch(processDataError(new Error('no simdata to process')));
        }

        if (!simData.data) {
            return dispatch(processDataError(new Error('Data not fetched yet for simulation: ' + simulationName)));
        }

        if (simData.processedData) {
            return dispatch(processDataFinished(simulationName, simData.processedData));
        }

        // TESTING
        // var processDataWorker = new ProcessDataWorker();
        var processDataWorker = new TestWorker();
        // var processDataWorker = new Worker('js/dist/test.js');

        processDataWorker.onmessage = function(e) {
            var processedData = e.data;
            dispatch(processDataFinished(simulationName, processedData));
        };
        processDataWorker.onerror = function(err) {
            console.log('err msg:  ' + err.message);
            console.log('err file: ' + err.filename);
            console.log('err line: ' + err.lineno);
            console.log('err col:  ' + err.colno);
            dispatch(processDataError(simulationName, err));
        };
        processDataWorker.postMessage({
            simulationName: simulationName,
            data: simData.data
        });

    };
};

export function processDataFinished(simulationName, processedData) {
    return {
        type: Types.PROCESS_DATA_FINISH,
        name: simulationName,
        processedData: processedData
    };
};

export function processDataError(simulationName, error) {
    return {
        type: Types.PROCESS_DATA_ERROR,
        name: simulationName,
        error: error
    };
};