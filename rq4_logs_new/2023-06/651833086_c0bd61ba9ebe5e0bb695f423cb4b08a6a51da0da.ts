// export function formatDateString(date: Date): string {
//   return (
//     "on " +
//     date.toLocaleDateString() +
//     " at " +
//     date.toLocaleTimeString([], {
//       hour: "numeric",
//       minute: "2-digit",
//     })
//   );
// }

export function formatDateString(dateInput: Date): string {
  const currentDate = new Date();

  // Calculate the time difference in milliseconds
  const timeDiff = currentDate.getTime() - dateInput.getTime();

  // Convert the time difference to minutes, hours, days, months, and years
  const minutes = Math.floor(timeDiff / (1000 * 60));
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const months = Math.floor(days / 30);
  const years = Math.floor(months / 12);

  if (minutes < 1) {
    return "just now";
  } else if (minutes < 60) {
    return `${minutes} minute${minutes > 1 ? "s" : ""} ago`;
  } else if (hours < 24) {
    return `${hours} hour${hours > 1 ? "s" : ""} ago`;
  } else if (days < 30) {
    return `${days} day${days > 1 ? "s" : ""} ago`;
  } else if (months < 12) {
    return `${months} month${months > 1 ? "s" : ""} ago`;
  } else {
    return `${years} year${years > 1 ? "s" : ""} ago`;
  }
}