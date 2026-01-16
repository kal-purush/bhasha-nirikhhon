import type { QueryDocumentSnapshot, DocumentData } from "firebase/firestore";
import sortBy from "lodash.sortby";
import omit from "lodash.omit";
import dayjs from "dayjs";
// @ts-ignore
//import pluck from "lodash.pluck";

import {
  collection,
  doc,
  addDoc,
  updateDoc,
  getDocs,
  deleteDoc,
  query,
  where,
} from "firebase/firestore";

import { db } from "@/firebase";
const repetitionTable = "repetition";

const repetitionTableRef = collection(db, repetitionTable);
const repetitionRef = (id: string) => doc(db, repetitionTable, id);

import type { RepetitionRecord } from "@/types";

const withId = (item: QueryDocumentSnapshot<DocumentData>): RepetitionRecord =>
  ({ ...item.data(), id: item.id } as RepetitionRecord);
const withoutId = (item: RepetitionRecord): RepetitionRecord => {
  return omit(item, ["id"]);
};

export async function create(
  repetition: RepetitionRecord
): Promise<RepetitionRecord> {
  return addDoc(repetitionTableRef, withoutId(repetition)).then(({ id }) => ({
    ...repetition,
    id,
  }));
}

export async function update(repetition: RepetitionRecord): Promise<void> {
  if (!repetition.id) return;

  return updateDoc(repetitionRef(repetition.id), withoutId(repetition));
}

export async function destroy(repetition: RepetitionRecord): Promise<void> {
  if (!repetition.id) return;

  return deleteDoc(repetitionRef(repetition.id));
}

export async function find(
  userId: string,
  activityId: string
): Promise<RepetitionRecord | null> {
  const response = await getDocs(
    query(
      repetitionTableRef,
      where("userId", "==", userId),
      where("activityId", "==", activityId)
    )
  );
  const [first] = response.docs.map(withId);
  return first || null;
}

export async function findOrCreate(
  userId: string,
  activityId: string
): Promise<RepetitionRecord> {
  const current = await find(userId, activityId)

  if (current) {
    return current
  }

  return await create({
    userId, activityId, startedAt: dayjs().valueOf(), repeatCount: 0
  })
}

export async function register(userId: string,
  activityId: string
): Promise<RepetitionRecord> {
  const current = await findOrCreate(userId, activityId)
  const interval = [1, 1, 1]
  const span = 'minute'

  if (current.scheduledAt && dayjs(current.scheduledAt).isAfter(dayjs())) {
    return current; // Can't repeat before scheduled time
  }

  if (current.finishedAt) {
    return current; // Can't repeat finished
  }

  if (interval.length < current.repeatCount) {
    const next = interval[current.repeatCount]
    current.scheduledAt = dayjs().add(next, span).startOf(span).valueOf()
    current.repeatCount += 1
  } else {
    current.finishedAt = dayjs().valueOf()
  }
  await update(current)
  return current
}

export async function agenda(userId: string): Promise<RepetitionRecord[]> {
  const now = dayjs().valueOf()
  const response = await getDocs(
    query(repetitionTableRef, 
      where("userId", "==", userId), 
      where("scheduledAt", '<=', now),
      where("finishedAt", '==', null),
    )
  );
  return sortBy(response.docs.map(withId), ["scheduledAt"]);
}

export async function plan(userId: string): Promise<RepetitionRecord[]> {
  const now = dayjs().valueOf()
  const response = await getDocs(
    query(repetitionTableRef, 
      where("userId", "==", userId), 
      where("scheduledAt", '>', now),
      where("finishedAt", '==', null),
    )
  );
  return sortBy(response.docs.map(withId), ["scheduledAt"]);
}

export async function history(userId: string): Promise<RepetitionRecord[]> {
  const response = await getDocs(
    query(repetitionTableRef, 
      where("userId", "==", userId), 
      where("finishedAt", '!=', null),
    )
  );
  return sortBy(response.docs.map(withId), ["finishedAt"]);
}