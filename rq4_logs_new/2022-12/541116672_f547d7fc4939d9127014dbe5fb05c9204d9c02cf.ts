import { QuestionContext } from '@/pages/question/[id]';
import { db } from '../../firebase/clientApp';
import {
  collection,
  doc,
  getDoc,
  getDocs,
  runTransaction,
  writeBatch,
} from 'firebase/firestore';
import { useSession } from 'next-auth/react';
import { useContext, useMemo } from 'react';
import { CommentContext } from '../Comment';

const useMarkCommentAsSolution = () => {
  const comment = useContext(CommentContext);
  const question = useContext(QuestionContext);
  const { data: session } = useSession();

  const isSolution = useMemo(() => {
    if (!comment?.id || !question?.id) return;
    return comment?.id === question?.id;
  }, [comment?.id, question?.id]);

  const markCommentAsSolution = () => {
    if (!comment || !question || session) return;

    const batch = writeBatch(db);

    const questionRef = doc(db, 'question', question.id);
    const waitingDetailsRef = collection(
      db,
      'question',
      question.id,
      'waitingDetails'
    );

    try {
      if (isSolution) {
        runTransaction(db, async (transaction) => {
          // const waitingDetails = await transaction.set
        });

        return;
      }

      if (!question.solvedCommentId) {
        return;
      }

      // do something
    } catch (e) {
      console.log(e);
    }
  };

  return { isSolution, markCommentAsSolution };
};

export { useMarkCommentAsSolution };