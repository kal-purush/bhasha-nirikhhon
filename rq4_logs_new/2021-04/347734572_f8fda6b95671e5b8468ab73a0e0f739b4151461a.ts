interface Dates {
  today: string;
  tomorrow: string;
  dayAfterTomorrow: string;
  threeDaysAhead: string;
}

export const getDates = (): Dates => {
  const getDate = (timestamp = Date.now()): string =>
    new Date(timestamp).toISOString().split('T').shift();

  const today = getDate();
  const tomorrow = getDate(Date.now() + 24 * 3600 * 1000);
  const dayAfterTomorrow = getDate(Date.now() + 48 * 3600 * 1000);
  const threeDaysAhead = getDate(Date.now() + 72 * 3600 * 1000);

  return {
    today,
    tomorrow,
    dayAfterTomorrow,
    threeDaysAhead,
  };
};