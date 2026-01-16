import firebase from 'firebase';

import { db } from '../config/Firebase';
import Task from '../types/task';

type ReturnValue = {
  firestoreAddTask: (
    task: Task,
  ) => Promise<
    firebase.firestore.DocumentReference<firebase.firestore.DocumentData>
  >;
};

const useFirestoreAddTask = (uid: string): ReturnValue => {
  const firestoreAddTask = (
    task: Task,
  ): Promise<
    firebase.firestore.DocumentReference<firebase.firestore.DocumentData>
  > =>
    db
      .collection('tasks')
      .doc(uid)
      .collection('task')
      .add({
        title: task.title,
        expirationDate: task.expirationDate.format('YYYY-MM-DD'),
        dueDate: task.dueDate.format('YYYY-MM-DD'),
      });

  return { firestoreAddTask };
};

export default useFirestoreAddTask;