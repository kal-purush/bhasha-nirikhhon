import { toast } from 'sonner';

import { AuthErrorCodes } from 'firebase/auth';
import isFirebaseError from './isFirebaseError';

export default function handleFirebaseError(error: unknown) {
  if (isFirebaseError(error)) {
    switch (error.code) {
      case AuthErrorCodes.ADMIN_ONLY_OPERATION:
        toast.error('This operation is only allowed to administrators');
        break;

      case AuthErrorCodes.EMAIL_EXISTS:
        toast.error('Email already exists');
        break;

      case AuthErrorCodes.INVALID_LOGIN_CREDENTIALS:
        toast.error('Email or password is incorrect');
        break;

      case AuthErrorCodes.TOO_MANY_ATTEMPTS_TRY_LATER:
        toast.error('Too many attempts, try again later');
        break;

      default:
        console.log(error);
        break;
    }
  }
}