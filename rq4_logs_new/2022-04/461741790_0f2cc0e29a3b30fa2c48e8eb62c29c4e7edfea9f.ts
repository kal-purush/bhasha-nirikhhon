import { FirebaseError } from "firebase/app";
import { Dispatch, SetStateAction } from "react";

export const handleAuthError = (
  reason: FirebaseError,
  setErrorMessage: Dispatch<SetStateAction<string>>,
  setIsEmailAlreadyInUse: Dispatch<SetStateAction<boolean>>,
  setIsEmailFormatInvalid: Dispatch<SetStateAction<boolean>>,
  setIsEmailNotFound: Dispatch<SetStateAction<boolean>>,
  setIsPasswordError: Dispatch<SetStateAction<boolean>>,
) => {

  switch (reason?.code) {
    case "auth/email-already-exists":
    case "auth/email-already-in-use":
      setIsEmailAlreadyInUse(true);
      setErrorMessage("このメールアドレスは既に登録済みです");
      break;

    case "auth/id-token-expired":
      setErrorMessage("内部エラーです。再読み込みして下さい (トークン期限切れ)");
      break;

    case "auth/id-token-revoked":
      setErrorMessage("内部エラーです。再読み込みして下さい (トークンが取り消されています)");
      break;

    case "auth/internal-error":
      setErrorMessage(`内部エラーです。しばらく時間をおいて再読み込みして下さい (Firebase内部エラー  Error Message:${reason.message})`);
      break;

    case "auth/invalid-email":
      setIsEmailFormatInvalid(true);
      setErrorMessage("このメールアドレスは形式が正しくありません");
      break;

    case "auth/operation-not-allowed":
      setErrorMessage("内部エラーです。再読み込みして下さい (指定されたログインプロバイダは無効化されています)");
      break;

    case "auth/project-not-found":
      setErrorMessage("内部エラーです。しばらく時間をおいて再読み込みして下さい (Firebaseプロジェクトが見つかりませんでした)");
      break;

    case "auth/session-cookie-expired":
      setErrorMessage("内部エラーです。再読み込みして下さい (Cookie期限切れ)");
      break;

    case "auth/session-cookie-revoked":
      setErrorMessage("内部エラーです。再読み込みして下さい (Cookieが取り消されています)");
      break;

    case "auth/uid-already-exists":
      setErrorMessage("内部エラーです。再度お試しください (UIDが既に存在します)");
      break;

    case "auth/unauthorized-continue-uri":
      setErrorMessage("許可されていないページからの操作です。ページのアドレスをよくお確かめください。");
      break;

    case "auth/user-not-found":
      setIsEmailNotFound(true);
      setErrorMessage("アカウントが見つかりませんでした");
      break;

    case "auth/weak-password":
      setIsPasswordError(true);
      setErrorMessage("パスワードが脆弱です (6文字以上のパスワードを設定して下さい)");
      break;

    case "auth/wrong-password":
      setIsPasswordError(true);
      setErrorMessage("パスワードが間違っています");
      break;

    default:
      if (reason?.message !== undefined)
        setErrorMessage(`不明なエラーが発生しました (${reason.message})`);
      break;
  }
};