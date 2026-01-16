import {
  assertFails,
  assertSucceeds,
  initializeTestEnvironment,
  RulesTestEnvironment,
} from '@firebase/rules-unit-testing';
import firebase from 'firebase/compat/app';
import { datatype } from 'faker';
import { CreateNewItemFn, createNewItemFn } from './utils/common.utils';
import { updateDoc } from '@firebase/firestore';
import { doc, getDoc } from 'firebase/firestore';
import { Interval } from '../types/schedules';
import { getInterval } from './utils/interval.utils';

describe('Interval rules', () => {
  const token = datatype.uuid();
  const otherToken = datatype.uuid();
  const defaultScheduleId = datatype.uuid();
  const defaultIntervalId = datatype.uuid();

  const getPath = (
    userUid = token,
    scheduleId = defaultScheduleId,
    intervalId = defaultIntervalId
  ) => `companies/${userUid}/schedules/${scheduleId}/intervals/${intervalId}`;

  let env: RulesTestEnvironment;
  let firestore: firebase.firestore.Firestore;
  let otherUserFirestore: firebase.firestore.Firestore;

  let createNewInterval: CreateNewItemFn<Interval>;

  beforeAll(async () => {
    env = await initializeTestEnvironment({ projectId: datatype.uuid() });
    firestore = env.authenticatedContext(token).firestore();
    otherUserFirestore = env.authenticatedContext(otherToken).firestore();
    createNewInterval = createNewItemFn({
      firestore,
      path: getPath(),
      getItemData: getInterval,
    });
  });

  beforeEach(async () => {
    await env.clearFirestore();
  });

  afterAll(async () => {
    await env.cleanup();
  });

  it('Should create new item', async () => {
    await createNewInterval();
  });

  it('Should update item', async () => {
    const [docRef] = await createNewInterval();
    const newData = getInterval();
    await assertSucceeds(updateDoc(docRef, newData));
  });

  it('Should throw error for create item for other user id', async () => {
    await createNewInterval({
      path: getPath(datatype.uuid()),
      withSuccess: false,
    });
  });

  it('Should throw error for update item for other user id', async () => {
    const [{ path }] = await createNewInterval();
    const docRef = doc(otherUserFirestore, path);
    await assertFails(updateDoc(docRef, getInterval()));
  });

  it('Should get my item data', async () => {
    const [docRef] = await createNewInterval();
    await assertSucceeds(getDoc(docRef));
  });

  it('Should throw error for get other item data', async () => {
    const [{ path }] = await createNewInterval();
    const docRef = doc(otherUserFirestore, path);
    await assertFails(getDoc(docRef));
  });

  it('Should throw error for get all items', async () => {
    await createNewInterval();
    await assertFails(
      firestore
        .collection(
          `companies/${otherToken}/schedules/${defaultScheduleId}/intervals`
        )
        .get()
    );
  });

  it('Should return all items', async () => {
    await createNewInterval();
    await assertSucceeds(
      firestore
        .collection(
          `companies/${token}/schedules/${defaultScheduleId}/intervals`
        )
        .get()
    );
  });
});