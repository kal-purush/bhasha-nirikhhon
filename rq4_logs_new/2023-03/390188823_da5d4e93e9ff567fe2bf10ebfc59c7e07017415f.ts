import * as functions from 'firebase-functions';
import { ClassEnrollmentDbo } from '@sol/classes/enrollment/repository';
import { DatabaseUtility } from '@sol/firebase/database';
import * as admin from 'firebase-admin';

export const addEmailToSolsticeList = functions.firestore
    .document('enrollment/{enrollmentId}')
    .onCreate(async (documentSnapshot) => {
        const enrollmentRecord = documentSnapshot.data() as ClassEnrollmentDbo;
        if (
            enrollmentRecord.status === 'enrolled' &&
            !!enrollmentRecord.transactionId &&
            enrollmentRecord.isSignedUpForSolsticeEmails === true
        ) {
            await DatabaseUtility.getDatabase()
                .collection('mailing_lists')
                .doc('summer_solstice_2023')
                .update({
                    emails: admin.firestore.FieldValue.arrayUnion(
                        enrollmentRecord.contactEmail
                    ),
                });
        }
    });