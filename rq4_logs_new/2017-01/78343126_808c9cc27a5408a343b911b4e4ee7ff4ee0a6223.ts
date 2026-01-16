
import * as _ from "lodash";
import { StockpileState, qty, initialState as initialStockpileState } from "./stockpile";
import { decayPct } from "../entities/resources";

export type ConsumerResources = { [resourceId: string]: number };

export interface Consumer {
	stockpileId: string;
	consumerResources: ConsumerResources;
}

export interface ConsumerState extends StockpileState {
	consumers: { [id: string]: Consumer };
}

export const initialState: ConsumerState = {
	...initialStockpileState,
	consumers: {}
}

export function createConsumer(
	consumerId: string,
	{ stockpileId, consumerResources },
	state: ConsumerState
): ConsumerState {
	return {
		...state,
		consumers: {
			...state.consumers,
			[consumerId]: {
				stockpileId,
				consumerResources: consumerResources || {}
			}
		}
	}
}

//export function setConsumerResources(consumerId: string, consumerResources: ConsumerResources)

export function desiredResources(consumerId: string, state: ConsumerState) {
	const consumer = state.consumers[consumerId];
	if (!consumer) return {};

	return _.mapValues(consumer.consumerResources, (consumptionAmt, resourceId) => {
		const decayPctPerUnitTime = decayPct(resourceId, 1);

		// If good is highly perishable, we don't want to hoard it
		const hoardingFactor = Math.max(
			1,
			1 / (0.2 + Math.pow(decayPctPerUnitTime, 0.33))
		);

		return consumptionAmt * hoardingFactor;
	})
}