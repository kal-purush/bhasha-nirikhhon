export function dateString(val) {
  const date = new Date(val);
  const month = date.getMonth();
  let monthString = "";

  switch (month) {
    case 0:
      monthString = "Jan";
      break;
    case 1:
      monthString = "feb";
      break;
    case 2:
      monthString = "mar";
      break;
    case 3:
      monthString = "apr";
      break;
    case 4:
      monthString = "may";
      break;
    case 5:
      monthString = "jun";
      break;
    case 6:
      monthString = "jul";
      break;
    case 7:
      monthString = "avg";
      break;
    case 8:
      monthString = "sep";
      break;
    case 9:
      monthString = "oct";
      break;
    case 10:
      monthString = "nov";
      break;
    case 11:
      monthString = "dec";
      break;
  }

  return `${monthString} ${date.getDate()} ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`;
}