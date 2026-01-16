import { FirebaseErrorCode } from "@/types/enums/firebase";
import type { FirebaseError } from "firebase/app";

export const getErrorMessage = (error: Error) => {
  if (error.name === "FirebaseError")
    return getFirebaseErrorMessage(error as FirebaseError);
  else return error.message;
};

const getFirebaseErrorMessage = (error: FirebaseError) => error.code;

export const isMultiFactorError = (error: unknown) => {
  return (
    (error as FirebaseError).code ===
    (FirebaseErrorCode.MULTI_FACTOR_AUTH_REQUIRED as string)
  );
};