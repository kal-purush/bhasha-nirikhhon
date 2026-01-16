const useCalendar = ({
    currentMonthFirst,
    currentMonthLast,
    previousMonthLast
}) => {

  // 날짜 배열에 넣기
  let dayArray: number[] = [];
  // 달력 한 면의 색을 채워넣는 배열
  let colorArray: boolean[] = [];
  for (
    let i = previousMonthLast.getDate() - (currentMonthFirst.getDay() - 1);
    i <= previousMonthLast.getDate();
    i++
  ) {
    dayArray.push(i);
    colorArray.push(false);
  }
  for (
    let i = currentMonthFirst.getDate();
    i <= currentMonthLast.getDate();
    i++
  ) {
    dayArray.push(i);
    colorArray.push(true);
  }
  for (let i = 1; i < 7 - currentMonthLast.getDay(); i++) {
    dayArray.push(i);
    colorArray.push(false);
  }

  return {};
};

export default useCalendar;