import { useEffect, useState } from 'react';
import firebase from 'gatsby-plugin-firebase';

function getPostLikeRef(slug: string) {
  return firebase.firestore().collection('post-likes').doc(slug);
}

export function usePostLikes(slug: string) {
  const [likes, setLikes] = useState<number>(0);

  useEffect(() => {
    const unsubscribe = getPostLikeRef(slug).onSnapshot((doc) => {
      const blog = doc.data();

      if (!doc.exists) {
        // ないので作成する
        doc.ref.set({ count: 0 });
      }

      setLikes(blog?.count ?? 0);
    });

    return () => {
      unsubscribe();
    };
  }, []);

  function incrementLike() {
    getPostLikeRef(slug).update({
      count: firebase.firestore.FieldValue.increment(1),
    });
  }

  return [likes, incrementLike] as const;
}