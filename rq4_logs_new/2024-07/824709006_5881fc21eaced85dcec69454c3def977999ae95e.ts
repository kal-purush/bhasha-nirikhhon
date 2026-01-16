import { useEffect, useState } from "react";
import { Event } from "../context/Events";

export default function useLocalStorage(key: string, initialValue: Event[]) {
  const [value, setValue] = useState<Event[]>(() => {
    const jsonValue = localStorage.getItem(key);
    if (jsonValue == null) return initialValue;
    return (JSON.parse(jsonValue) as Event[]).map((event) => {
      if (event.date instanceof Date) return event;
      return { ...event, date: new Date(event.date) };
    });
  });

  useEffect(() => {
    localStorage.setItem(key, JSON.stringify(value));
  }, [key, value]);

  return [value, setValue] as const;
}