'use server';

import * as firestore from '@/lib/firebase/firestore';
import { getAuthenticatedAppForUser } from '@/lib/firebase/serverApp';
import type { Nullable } from '@/lib/utils/typeHelpers';
import { getFirestore } from 'firebase/firestore';
import { redirect } from 'next/navigation';

export const deleteUserTracker = async (
  userTrackerId: Nullable<string>,
  _e: FormData,
) => {
  const { firebaseServerApp, currentUser } = await getAuthenticatedAppForUser();
  const db = getFirestore(firebaseServerApp);
  const uid = currentUser?.uid;

  await firestore
    .deleteUserTracker({
      userTrackerId,
      db,
      uid,
    })
    .then(() => {
      // TODO: toast
      // queryClient.invalidateQueries({ queryKey: ['getProjects'] });
      redirect('/');
    });
};