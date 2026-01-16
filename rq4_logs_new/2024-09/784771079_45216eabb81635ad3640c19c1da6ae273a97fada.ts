import { useState, useCallback, useEffect } from 'react';

export const useAvailability = (initialAvailability: string = '') => {
  const [selectedDays, setSelectedDays] = useState<string[]>([]);
  const [selectedTimes, setSelectedTimes] = useState<Record<string, string[]>>({});

  // Parse initial availability string
  useEffect(() => {
    if (initialAvailability) {
      try {
        const availabilityObj = JSON.parse(initialAvailability);
        setSelectedDays(Object.keys(availabilityObj));
        setSelectedTimes(availabilityObj);
      } catch (error) {
        console.error('Error parsing initial availability:', error);
      }
    }
  }, [initialAvailability]);

  const toggleDay = useCallback((day: string) => {
    setSelectedDays((prev) => {
      if (prev.includes(day)) {
        const newDays = prev.filter((d) => d !== day);
        setSelectedTimes((prevTimes) => {
          const { [day]: _, ...rest } = prevTimes;
          return rest;
        });
        return newDays;
      } else {
        return [...prev, day].sort((a, b) =>
          ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].indexOf(a) -
          ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].indexOf(b)
        );
      }
    });
  }, []);

  const changeTime = useCallback((day: string, startTime: string, endTime: string) => {
    setSelectedTimes((prev) => ({
      ...prev,
      [day]: [startTime, endTime]
    }));
  }, []);

  const getAvailabilityString = useCallback(() => {
    return JSON.stringify(selectedTimes);
  }, [selectedTimes]);

  return {
    selectedDays,
    selectedTimes,
    toggleDay,
    changeTime,
    getAvailabilityString,
  };
};