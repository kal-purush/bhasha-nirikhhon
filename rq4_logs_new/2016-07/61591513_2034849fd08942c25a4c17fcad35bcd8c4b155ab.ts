import {DoseEvent} from "../dose-event/dose-event";
import {DOSE_MEDICATIONS} from "../dose-medication-service/dose-medication-mocks";

const eightAM: Date = new Date();
eightAM.setHours(8);


export var DOSE_EVENTS: Array<DoseEvent> = [
    {
        id: 3,
        med: DOSE_MEDICATIONS[0],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 2,
        med: DOSE_MEDICATIONS[1],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 1,
        med: DOSE_MEDICATIONS[2],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    }
];