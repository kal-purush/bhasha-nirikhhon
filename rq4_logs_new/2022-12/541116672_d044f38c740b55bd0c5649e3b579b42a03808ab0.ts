import { CollectionEnum, db } from '@/firebase/.';
import { collection, getDocs, onSnapshot } from 'firebase/firestore';
import { useSession } from 'next-auth/react';
import { useEffect, useState } from 'react';
import { VoteDetail } from '..';

export const useGetVotedComments = () => {
  const { data: session } = useSession();
  const [votedComments, setVotedComments] = useState<Map<string, VoteDetail>>(
    new Map()
  );

  useEffect(() => {
    // const getVotedCommnents = async () => {
    //   if (!session) return;
    //
    //   try {
    //     const votedCommentsRef = collection(
    //       db,
    //       CollectionEnum.USERS,
    //       session.user.uid,
    //       CollectionEnum.VOTEDCOMMENTS
    //     );
    //
    //     const snapshot = await getDocs(votedCommentsRef);
    //     setVotedComments(
    //       new Map(
    //         snapshot.docs.map((doc) => [doc.id, doc.data() as VoteDetail])
    //       )
    //     );
    //   } catch (e) {
    //     console.log(e);
    //   }
    // };
    //
    // getVotedCommnents();

    if (!session) return;

    const votedCommentsRef = collection(
      db,
      CollectionEnum.USERS,
      session.user.uid,
      CollectionEnum.VOTEDCOMMENTS
    );

    onSnapshot(votedCommentsRef, (snapshot) => {
      setVotedComments(
        new Map(
          snapshot.docs
            .filter((doc) => doc.data())
            .map((doc) => [doc.id, doc.data() as VoteDetail])
        )
      );
    });
  }, [session]);

  return votedComments;
};