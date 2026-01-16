import {DoseEvent} from "../dose-event/dose-event";
import {DOSE_MEDICATIONS} from "../dose-medication-service/dose-medication-mocks";

const eightAM: Date = new Date();
eightAM.setHours(8);

export var DOSE_EVENTS2: Array<DoseEvent> = [
    {
        id: 6,
        med: DOSE_MEDICATIONS[0],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 5,
        med: DOSE_MEDICATIONS[1],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 4,
        med: DOSE_MEDICATIONS[2],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    }
];