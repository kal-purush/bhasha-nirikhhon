import {DoseEvent} from "../dose-event/dose-event";
import {DOSE_MEDICATIONS} from "../dose-medication-service/dose-medication-mocks";

const eightAM: Date = new Date();
eightAM.setHours(11);

export var DOSE_EVENTS3: Array<DoseEvent> = [
    {
        id: 91,
        med: DOSE_MEDICATIONS[2],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 92,
        med: DOSE_MEDICATIONS[1],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    },
    {
        id: 93,
        med: DOSE_MEDICATIONS[0],
        scheduledDateTime: eightAM,
        actionDateTime: null,
        action: ""
    }
];