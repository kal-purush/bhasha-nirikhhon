export function getCurrentDateString() {
  const date = new Date();
  return (
    date.toDateString() +
    ' ' +
    date.toLocaleTimeString(undefined, {
      hour12: false,
    })
  );
}

export function getAge(dateString: string) {
  return Math.floor((new Date().getTime() - new Date(dateString).getTime()) / 3.154e10);
}