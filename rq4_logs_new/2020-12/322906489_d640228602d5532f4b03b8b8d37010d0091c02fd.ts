import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
admin.initializeApp();

const auth = admin.auth();

interface ClaimsDocumentData extends admin.firestore.DocumentData {
    _lastCommitted?: admin.firestore.Timestamp
}

export const mirrorCustomClaims = functions.firestore.document('user-claims/{uid}')
    .onWrite(async (change, context) => {
        const beforeData: ClaimsDocumentData = change.before.data() || {};
        const afterData: ClaimsDocumentData = change.after.data() || {};

        // skip updates where _lastCommitted field changed
        const skipUpdate = beforeData._lastCommitted && afterData._lastCommitted &&
                           !beforeData._lastCommitted.isEqual(afterData._lastCommitted);
        if (skipUpdate) {
            return;
        }

        // Create a new JSON payload and check that it's under 1000 characters
        const { _lastCommitted, ...newClaims } = afterData;
        const stringifiedClaims = JSON.stringify(newClaims);
        if (stringifiedClaims.length > 1000) {
            console.error("Custom claims object string > 1000 characters");
            return;
        }

        const uid = context.params.uid;
        await auth.setCustomUserClaims(uid, newClaims);
        await change.after.ref.update({
            _lastCommitted: admin.firestore.FieldValue.serverTimestamp(),
            ...newClaims,
        });
    });