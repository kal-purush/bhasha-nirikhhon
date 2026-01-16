import { format, formatDistanceToNow, differenceInDays } from 'date-fns';

export function formatTimeDistance(date: string) {
  const dateObj = new Date(date);
  const now = new Date();

  const diffInDays = differenceInDays(now, dateObj);

  if (diffInDays >= 30) {
    return format(dateObj, 'yyyy.MM.dd');
  }

  const distance = formatDistanceToNow(new Date(date));

  return distance
    .replace(/about /g, '')
    .replace(/less than a minute/g, '방금 전')
    .replace(/(\d+) minutes?/g, '$1분 전')
    .replace(/(\d+) hours?/g, '$1시간 전')
    .replace(/(\d+) days?/g, '$1일 전');
}