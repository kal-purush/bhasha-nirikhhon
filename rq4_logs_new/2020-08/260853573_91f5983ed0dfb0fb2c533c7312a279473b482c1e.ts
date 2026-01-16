import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import { shouldEventRun, markEventTried } from './util.ts/firebase-util';

const db = admin.firestore();

export const countUpLiked = functions
  .region('asia-northeast1')
  .firestore.document('posts/{id}/likedUserIds/{userId}')
  .onCreate(async (snap, context) => {
    const eventId = context.eventId;
    const id = context.params.id;
    const should = await shouldEventRun(eventId);
    if (should) {
      const postSnapShot = await db.doc(`posts/${id}`).get();
      console.log(postSnapShot);

      if (postSnapShot.exists) {
        await postSnapShot.ref.update(
          'likeCount',
          admin.firestore.FieldValue.increment(1)
        );
      } else {
        console.log(`Post ${id}`);
      }

      return markEventTried(eventId);
    } else {
      return true;
    }
  });

export const countDownLiked = functions
  .region('asia-northeast1')
  .firestore.document('posts/{id}/likedUserIds/{userId}')
  .onDelete(async (snap, context) => {
    const eventId = context.eventId;
    const should = await shouldEventRun(eventId);
    const id = context.params.id;

    if (should) {
      const snapShot = await db.doc(`posts/${id}`).get();
      if (snapShot.exists) {
        await snapShot.ref.update(
          'likeCount',
          admin.firestore.FieldValue.increment(-1)
        );
      } else {
        console.log(`User: does not exist.`);
      }

      return markEventTried(eventId);
    } else {
      return true;
    }
  });