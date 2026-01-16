import type { QueryDocumentSnapshot, DocumentData } from "firebase/firestore";

import {
  collection,
  doc,
  addDoc,
  updateDoc,
  getDocs,
  getDoc,
  deleteDoc,
  query,
  where,
  limit
} from "firebase/firestore";

import omit from "lodash.omit";

import { db } from "@/firebase";
const usersTable = "users";

const usersTableRef = collection(db, usersTable);
const usersRef = (id: string) => doc(db, usersTable, id);

import type { User } from "@/types";

const withId = (item: QueryDocumentSnapshot<DocumentData>): User =>
  ({ ...item.data(), id: item.id } as User);
const withoutId = (item: User): User => {
  return omit(item, ["id"]);
};

export async function fetchAll(): Promise<User[]> {
  const response = await getDocs(query(usersTableRef));
  return response.docs.map(withId);
}

export async function fetchOne(id: string): Promise<User | null> {
  const response = await getDoc(usersRef(id));
  if (response.exists()) {
    return withId(response);
  }
  return null;
}

export async function findWithUid(uid: string): Promise<User | null> {
  const response = await getDocs(
    query(usersTableRef, where("uid", "==", uid), limit(1))
  );
  return response.docs.map(withId).at(0) || null;
}

export async function fechAccess(uid: string): Promise<string | null> {
  const accessRef = collection(db, "access");
  const accessResponse = await getDocs(
    query(accessRef, where("uid", "==", uid), limit(1))
  );
  return accessResponse.docs.at(0)?.data()?.access || "";
}

export async function create(user: User): Promise<User> {
  return addDoc(usersTableRef, withoutId(user)).then(({ id }) => ({
    ...user,
    id,
  }));
}

export async function update(user: User): Promise<void> {
  if (!user.id) return;

  return updateDoc(usersRef(user.id), withoutId(user));
}

export async function destroy(user: User): Promise<void> {
  if (!user.id) return;

  return deleteDoc(usersRef(user.id));
}
