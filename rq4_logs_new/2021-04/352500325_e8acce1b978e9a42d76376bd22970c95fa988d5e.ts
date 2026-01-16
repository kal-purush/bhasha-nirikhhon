import dayjs from 'dayjs';

import { db } from '../config/Firebase';

type returnType = {
  firestoreUpdateTitle: (taskId: string, title: string) => Promise<void>;
  firestoreUpdateExpirationDate: (
    taskId: string,
    dueDate: dayjs.Dayjs,
  ) => Promise<void>;
  firestoreUpdateDueDate: (
    taskId: string,
    dueDate: dayjs.Dayjs,
  ) => Promise<void>;
  firestoreUpdateMemo: (taskId: string, memo: string) => Promise<void>;
};

const useFirestoreUpdateTask = (uid: string): returnType => {
  const firestoreUpdateTitle = (taskId: string, title: string) => {
    const result = db
      .collection('tasks')
      .doc(uid)
      .collection('todo')
      .doc(taskId)
      .update({ title });

    return result;
  };

  const firestoreUpdateExpirationDate = (
    taskId: string,
    expirationDate: dayjs.Dayjs,
  ) => {
    const result = db
      .collection('tasks')
      .doc(uid)
      .collection('todo')
      .doc(taskId)
      .update({ expirationDate });

    return result;
  };

  const firestoreUpdateDueDate = (taskId: string, dueDate: dayjs.Dayjs) => {
    const result = db
      .collection('tasks')
      .doc(uid)
      .collection('todo')
      .doc(taskId)
      .update({ dueDate });

    return result;
  };

  const firestoreUpdateMemo = (taskId: string, memo: string) => {
    const result = db
      .collection('tasks')
      .doc(uid)
      .collection('todo')
      .doc(taskId)
      .update({ memo });

    return result;
  };

  return {
    firestoreUpdateTitle,
    firestoreUpdateExpirationDate,
    firestoreUpdateDueDate,
    firestoreUpdateMemo,
  };
};

export default useFirestoreUpdateTask;