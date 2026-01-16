import { addDays, set } from 'date-fns';

const getDate = ({
  numberOfDays,
  currentDate,
}: {
  numberOfDays: number;
  currentDate: Date;
}) => {
  const date = addDays(currentDate, numberOfDays);
  return set(date, { hours: 0, minutes: 0, seconds: 0, milliseconds: 0 });
};

export const getEventsByDate = (filterBy: string | undefined) => {
  const currentDate = new Date();

  if (filterBy === '7days') {
    return getDate({ currentDate: currentDate, numberOfDays: 7 });
  }
  if (filterBy === '30days') {
    return getDate({ currentDate: currentDate, numberOfDays: 30 });
  }
  if (filterBy === '90days') {
    return getDate({ currentDate: currentDate, numberOfDays: 90 });
  }
  return undefined;
};