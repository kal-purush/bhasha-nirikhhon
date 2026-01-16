import { messaging, getToken } from '@/lib/firebase';

export async function handleFcmToken(adminId: number) {
  if (typeof window === 'undefined') {
    console.warn('서버 측에서는 FCM 토큰을 처리할 수 없습니다.');
    return;
  }

  try {
    if (messaging) {
      console.log("FCM 토큰을 가져오는 중...");
      const fcmToken = await getToken(messaging);
      if (fcmToken) {
        console.log("FCM 토큰:", fcmToken);
        // 서버에 FCM 토큰 전송
        await fetch(`/api/login_api/fcm/save-fcm-token`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ adminId, fcmToken }),
        });
      } else {
        console.warn('FCM 토큰을 얻을 수 없습니다.');
      }
    } else {
      console.warn('Firebase Messaging이 초기화되지 않았습니다.');
    }
  } catch (error) {
    console.error('FCM 토큰 처리 중 오류 발생:', error);
  }
  
}