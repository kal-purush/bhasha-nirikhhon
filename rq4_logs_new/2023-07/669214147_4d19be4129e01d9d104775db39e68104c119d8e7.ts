import { RequestType, ResponseType } from "../server";
import easService from "../services/eas-service";
import pushService from "../services/push-service";

const HTTP_ERROR_STATUS = 400;
const HTTP_SERVER_ERROR = 500

type subscriptionBodyType = {
    newsletterOwner: string;
    newsletterNonce: string;
    recipient: string;
};

//Generate attestation when subscription is payed by safeonramp callback
async function subscriptionNewsletter(
    request: RequestType,
    response: ResponseType
) {
    try {

        const attestationUID = await easService.generateAttestation(request.body.newsletterOwner, request.body.newsletterNonce, request.body.recipient, 1) //TODO month

        return response.send(attestationUID);
    } catch (error: any) {
        response.status(HTTP_ERROR_STATUS);

        return response.send(error.response);
    }
}

//Send notificatios to list addresses when send a new newsletter
async function newsletterPushNotification(
    request: RequestType,
    response: ResponseType
) {
    try {

        await pushService.sendPushNotification(request.body.addresses,request.body.owner,request.body.newsletterTitle, request.body.newsletterText)

        return response.send("OK");

    } catch (error: any) {
        response.status(HTTP_ERROR_STATUS);

        return response.send(error.response);
    }
}

async function getActivedSubscriptors(request: RequestType, response: ResponseType) {

    try{

        const activedAddress = await easService.getActivedSubscriptors(request.query.newsletterOwner as string, request.query.newsletterNonce as string)

        return response.send(activedAddress)

    } catch(error: any){
        response.status(HTTP_ERROR_STATUS);

        return response.send(error.response);
    }
    
}

async function getMyNewslettersSubscription(request: RequestType, response: ResponseType){

    try {

        console.log(request.params)

    }catch(error: any){
        response.status(HTTP_SERVER_ERROR);

        return response.send(error.response);
    }

}


const newsletterController = {
    subscriptionNewsletter,
    newsletterPushNotification,
    getActivedSubscriptors,
    getMyNewslettersSubscription
};

export default newsletterController;