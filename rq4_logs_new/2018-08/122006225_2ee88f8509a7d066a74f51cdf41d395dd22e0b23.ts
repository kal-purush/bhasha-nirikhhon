import {HttpsFunction} from "firebase-functions";
import * as admin from "firebase-admin";

const functions = require('firebase-functions');
try {
    admin.initializeApp(functions.config().firebase);
} catch (e) {
}
const db = admin.database();

export default class DeleteMessageWhenChatDeletedClass {

    public static deleteMessageWhenChatDeletedClassCloudFunction: HttpsFunction = functions.database.ref('/chats/{chatUid}/')
        .onDelete((snapshot, context) => {
                try {
                    const chatUid = context.params.chatUid;
                    return db.ref('/messages/' + chatUid).remove();
                } catch (error) {
                    console.error(error);
                    return null;
                }
            }
        );
}