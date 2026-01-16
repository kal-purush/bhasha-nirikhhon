import { Schema } from "jsonschema";

/**
 * Corresponding schema can be found at {@link NightSleepSchema}
 */
export interface INightSleep {
    id: number;
    night: string;
    sleepEndOffset: number;
    sleepEndTime: string;
    sleepGoal: number;
    sleepRating: null | "SLEPT_NEITHER_BAD_NOR_WELL";
    sleepStartOffset: number;
    sleepStartTime: string;
}

/**
 * Schema for {@link INightSleep}
 */
export const NightSleepSchema: Schema = {
    id: "/NightSleep",
    properties: {
        id: {
            type: "number",
        },
        night: {
            type: "string",
        },
        sleepEndOffset: {
            type: "number",
        },
        sleepEndTime: {
            type: "string",
        },
        sleepGoal: {
            type: "number",
        },
        sleepRating: {
            type: "string",
        },
        sleepStartOffset: { type: "number" },
        sleepStartTime: {
            type: "string",
        },
    },
    type: "object",
};