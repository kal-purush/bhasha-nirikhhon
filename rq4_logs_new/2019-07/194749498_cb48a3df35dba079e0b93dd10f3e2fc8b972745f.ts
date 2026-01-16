export const getTimePublishedFormat = (milliseconds: number): string => {
  const minutes: number = Math.floor(milliseconds / 1000 / 60);
  const hours: number = Math.floor(minutes / 60);
  const days: number = Math.floor(hours / 24);
  let timeAgoString: string;

  if (days > 0) {
    timeAgoString = days === 1 ? 'day ago' : 'days ago';
    return `${days} ${timeAgoString}`;
  } else if (hours > 0) {
    timeAgoString = hours === 1 ? 'hr ago' : 'hrs ago';
    return `${hours} ${timeAgoString}`;
  }
  timeAgoString = minutes === 1 ? 'min ago' : 'mins ago';
  return `${minutes} ${timeAgoString}`;
};