import { useState } from 'react';

const useNotification = () => {
  const [isNotificationVisible, setNotificationVisible] = useState(false);

  const showNotification = (title: string, body: string) => {
    if (document.visibilityState != 'visible') {
      displayBrowserNotification(title, body);
    }

    setNotificationVisible(true);

    setTimeout(() => {
      setNotificationVisible(false);
    }, 3000); // Показываем кастомное уведомление в течение 3 секунд
  };

  const displayBrowserNotification = (title: string, body: string) => {
    if (!('Notification' in window)) {
      console.warn('Этот браузер не поддерживает уведомления на рабочем столе.');
      return;
    }

    if (Notification.permission !== 'granted') {
      Notification.requestPermission().then((permission) => {
        if (permission === 'granted') {
          new Notification(title, { body });
        } else {
          console.warn('Разрешение на уведомления отклонено.');
        }
      });
    } else {
      new Notification(title, { body });
    }
  };

  return { showNotification, isNotificationVisible };
};

export default useNotification;