type DateOption = "yy-mm-dd" | "yyyy-mm-dd" | "ko";

export const convertUTCTimeToCustomString = (
  time: Date,
  option?: DateOption
) => {
  const [year, month, date] = [
    time.getFullYear(),
    time.getMonth() + 1,
    time.getDate()
  ];
  if (option === "yy-mm-dd") {
    return `${year}-${month < 10 ? "0" + month : month}-${
      date < 10 ? "0" + date : date
    }`;
  }
  if (option === "yyyy-mm-dd") {
    return `${year}-${month < 10 ? "0" + month : month}-${
      date < 10 ? "0" + date : date
    }`;
  }

  return `${year}년 ${month}월 ${date}일`;
};