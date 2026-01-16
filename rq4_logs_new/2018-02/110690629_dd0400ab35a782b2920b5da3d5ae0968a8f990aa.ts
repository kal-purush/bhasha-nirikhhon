import { defaultDatabase } from "./environment";

/**
 * 
 * @param message [user] : userId, step: number
 * 
 */
export function nextStep(message) {
    if (message.step === 1) {
        const json = {};
        json["capacity"] = 1000;
        json["currentCapacity"] = 1000;
        json["ore"] = "carbon";
        json["purity"] = 100;
        json["seed"] = "01230123";
        json["collectible"] = 0;
        json["timeToGo"] = 0;
        defaultDatabase.ref("users/" + message.user + "/asteroid").set(json);
    }
    defaultDatabase.ref("users/" + message.user + "/profile/step").set(message.step);
}


export function validationTutorial(userID, values: number) {
    defaultDatabase.ref("users/" + userID + "/profile/step").once('value').then((step) => {
        if (step.val() === 1 && values > 30) {
            defaultDatabase.ref("users/" + userID + "/profile/step").set(2);
        }

        if (step.val() === 2 && values > 150) {
            defaultDatabase.ref("users/" + userID + "/profile/step").set(3);
        }
    });
}