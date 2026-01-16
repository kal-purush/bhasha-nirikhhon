export function utcDateFormatter(date: Date): string {
  date.toUTCString();
  return (
    `${date.getUTCFullYear()}-${date.getUTCMonth()}-${date.getUTCDate()}` +
    `T${date.getUTCHours()}:${date.getUTCMinutes()}:${date.getUTCSeconds()}Z`
  );
}

export function dateInputFormatter(date: Date): string {
  const month = date.getMonth() < 10 ? `0${date.getMonth()}` : date.getMonth();
  const day = date.getDate() < 10 ? `0${date.getDate()}` : date.getDate();
  const hour = date.getHours() < 10 ? `0${date.getHours()}` : date.getHours();
  const minute =
    date.getMinutes() < 10 ? `0${date.getMinutes()}` : date.getMinutes();
  return `${date.getFullYear()}-${month}-${day}T${hour}:${minute}`;
}