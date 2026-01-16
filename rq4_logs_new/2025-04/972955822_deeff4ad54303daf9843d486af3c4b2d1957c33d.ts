import { 
  GoogleAuthProvider, 
  signInWithRedirect,
  signInWithPopup,
  getRedirectResult,
  signOut,
  onAuthStateChanged,
  type User,
  type Auth
} from 'firebase/auth';
import { auth } from './config';

// Google認証プロバイダーの作成
const googleProvider = new GoogleAuthProvider();

// 現在のドメインとFirebase Auth Domainが一致するかチェック
const shouldUseRedirect = (): boolean => {
  try {
    const currentDomain = window.location.hostname;
    const authDomain = import.meta.env.VITE_FIREBASE_AUTH_DOMAIN?.split('.')[0] || '';
    
    // 開発環境ではlocalhostなので、リダイレクトを使用
    if (currentDomain === 'localhost') {
      return true;
    }
    
    // 現在のドメイン名がFirebase Auth Domainと一致するかチェック
    const currentDomainBase = currentDomain.split('.')[0];
    return currentDomainBase === authDomain;
  } catch (error) {
    console.error('Error checking domain:', error);
    // エラー時はポップアップにフォールバック
    return false;
  }
};

// Googleプロバイダーでのサインイン（ドメインに基づいて方法を選択）
export const signInWithGoogle = async () => {
  try {
    if (shouldUseRedirect()) {
      // リダイレクト認証（同一ドメインの場合）
      console.log('Using redirect authentication');
      await signInWithRedirect(auth, googleProvider);
    } else {
      // ポップアップ認証（異なるドメインの場合）
      console.log('Using popup authentication');
      return await signInWithPopup(auth, googleProvider);
    }
  } catch (error) {
    console.error('Google sign-in error:', error);
    throw error;
  }
};

// リダイレクト結果の取得
export const getAuthRedirectResult = async () => {
  try {
    const result = await getRedirectResult(auth);
    return result;
  } catch (error) {
    console.error('Error getting redirect result:', error);
    throw error;
  }
};

// サインアウト
export const logOut = async () => {
  try {
    await signOut(auth);
  } catch (error) {
    console.error('Sign out error:', error);
    throw error;
  }
};

// 認証状態変更リスナーの設定
export const subscribeToAuthChanges = (
  callback: (user: User | null) => void
) => {
  return onAuthStateChanged(auth, callback);
};

// 現在のユーザーを取得
export const getCurrentUser = (): User | null => {
  return auth.currentUser;
};

// ユーザーがログインしているかどうかを確認
export const isUserLoggedIn = (): boolean => {
  return !!auth.currentUser;
};

// Authインスタンスを取得
export const getAuth = (): Auth => {
  return auth;
};