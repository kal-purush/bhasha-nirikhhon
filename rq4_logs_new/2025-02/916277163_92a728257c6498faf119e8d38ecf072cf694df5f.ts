import { useRef, useState } from 'react';

import Alert, { AlertData } from './alert';

export default function useAlertState() {
  const [alerts, setAlerts] = useState<Array<Alert>>([]);
  const timeouts = useRef<Map<string, NodeJS.Timeout>>(new Map());

  const addAlert = (alert: AlertData) => {
    const build = new Alert(alert);
    setAlerts((prev) => [...prev, build]);
  };

  const removeAlert = (alert: Alert) => {
    setAlerts((prev) =>
      prev.map((al) => {
        if (al.id === alert.id) {
          al.visible = false;
        }
        return al;
      }),
    );

    if (timeouts.current.has(alert.id)) {
      clearTimeout(timeouts.current.get(alert.id));
      timeouts.current.delete(alert.id);
    }

    const timeout = setTimeout(() => {
      setAlerts((prev) => prev.filter((al) => al.id !== alert.id));
      timeouts.current.delete(alert.id);
    }, 500);

    timeouts.current.set(alert.id, timeout);
  };

  return { alerts, addAlert, removeAlert };
}