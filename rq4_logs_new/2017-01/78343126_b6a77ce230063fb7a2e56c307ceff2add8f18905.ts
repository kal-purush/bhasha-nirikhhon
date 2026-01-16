
import { timeReducer } from "./time";
import { stockpileReducer } from "./stockpile";
import { consumerReducer } from "./consumer";
import { traderReducer } from "./trader";
import { industryReducer } from "./industry";
import { marketReducer } from "./market";

// Order is important here, as some actions are dependent on others
const reducers = [
	timeReducer,

	stockpileReducer,

	industryReducer,

	consumerReducer,
	traderReducer,
	marketReducer
]

export default function behavioursReducer(initialState, action) {
	// Yo dawg, heard you like reducers...
	return reducers.reduce(
		(state, reducer) => reducer(state, action),
		initialState
	);
}