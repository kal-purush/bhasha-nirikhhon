import axios from 'axios';
import { getDownloadURL, ref, uploadBytes } from 'firebase/storage';
import { useRouter } from 'next/router';
import { Dispatch, SetStateAction } from 'react';
import { useSetRecoilState } from 'recoil';
import { INIT_SIGNUP_DATA } from '../../consts/auth/stateInit';
import { getUserSession } from '../../hooks/useQueryUser';
import { storage } from '../../lib/firebase';
import { currentUserId } from '../../recoil/boardState';
import { generateFileName } from '../generateFileName';
import { toastSummary } from '../toastSummary';

type userInfo = {
  userName: string;
  email: string;
  password: string;
  url: string;
  fileName: string;
};

export const signupFeature = (
  signUpUser: userInfo,
  avatar: File | null,
  resetAvatar: Dispatch<SetStateAction<File | null>>,
  resetSignupUserValue: Dispatch<SetStateAction<userInfo>>,
  resetPreviewAvatar: Dispatch<SetStateAction<string>>,
  previewAvatar: string
) => {
  const router = useRouter();
  const { successCreateAcountToast } = toastSummary();
  const setUserId = useSetRecoilState(currentUserId);
  const signupInEmail = async () => {
    const postSignUp = async () => {
      console.log(signUpUser);
      return await axios.post(
        `${process.env.NEXT_PUBLIC_API_URL}/auth/signup`,
        {
          ...signUpUser,
        }
      );
    };
    try {
      if (avatar) {
        signUpUser.fileName = generateFileName(avatar.name);
        const refStorageAvatars = ref(
          storage,
          `avatars/${signUpUser.fileName}`
        );

        await uploadBytes(refStorageAvatars, avatar);
        signUpUser.url = await getDownloadURL(refStorageAvatars);

        await postSignUp();
      } else {
        signUpUser.url = await getDownloadURL(
          ref(storage, 'default_avatar/AdobeStock_64675209.jpeg')
        );
        await postSignUp();
      }
      resetSignupUserValue(INIT_SIGNUP_DATA);
      const user = await getUserSession();
      setUserId(user);
      successCreateAcountToast();
      URL.revokeObjectURL(previewAvatar);
      resetAvatar(null);
      resetPreviewAvatar('');
      await router.push('/board');
    } catch (err) {
      console.log(err);
    }
  };
  return { signupInEmail };
};