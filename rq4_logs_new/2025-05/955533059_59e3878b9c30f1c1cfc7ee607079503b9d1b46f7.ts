'use server';

import * as firestore from '@/lib/firebase/firestore';
import { getAuthenticatedAppForUser } from '@/lib/firebase/serverApp';
import { questionToFirestore } from '@/lib/utils/firebaseConverters';
import type { Question } from '@/lib/utils/types/Questions';
import { getFirestore } from 'firebase/firestore';
import { redirect } from 'next/navigation';
import { v4 as uuidv4 } from 'uuid';

const convertQuestionData = (
  data: Record<string, FormDataEntryValue>,
  questionList: Question[],
) => {
  return Object.keys(data).reduce(
    (acc, q) => {
      const question = questionList?.find(
        question => question.id === Number(q),
      );
      if (!data[q] || !question) return acc;
      return {
        ...acc,
        [q]: questionToFirestore({ type: question.type, value: data[q] }),
      };
    },
    // TODO
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    {} as Record<string, any>,
  );
};

export const upsertTracker = async (
  e: FormData,
  questionList: Question[],
  trackerId: number,
) => {
  const { firebaseServerApp, currentUser } = await getAuthenticatedAppForUser();
  const db = getFirestore(firebaseServerApp);
  const uid = currentUser?.uid;
  // TODO: validate data types, e.g. int !== float
  const data = Object.fromEntries(e);
  const converted = convertQuestionData(data, questionList);
  await firestore
    .upsertTracker({
      tracker: {
        id: uuidv4(),
        // ...(!editId && { created: new Date() }),
        created: Date.now(),
        modified: Date.now(),
        trackerId,
        data: converted,
      },
      db,
      uid,
    })
    .then(() => {
      // queryClient.invalidateQueries({ queryKey: ['getProjects'] });
      redirect('/');
    });
};