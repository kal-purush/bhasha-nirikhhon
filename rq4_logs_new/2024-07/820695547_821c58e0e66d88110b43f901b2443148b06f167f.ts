import moment from "moment";
import { useEffect, useState } from "react";
import { DaysGrid } from "../types/EventCalendarType";

export default function useEventCalendar() {
  const [date, setDate] = useState(moment());
  const [daysGrid, setDaysGrid] = useState<Array<DaysGrid>>([]);

  useEffect(() => {
    getMonthDaysGrid();
  }, [date]);

  const getMonthDaysGrid = () => {
    let totalNextMonthStartDays: number;
    const firstDayofMonth = date.clone().startOf("month");
    const lastDayofMonth = date.clone().endOf("month");
    const totalLastMonthFinalDays =
      firstDayofMonth.days() - 1 < 0 ? 6 : firstDayofMonth.days() - 1;

    const lastDayofMonthDays = lastDayofMonth.days();

    if (lastDayofMonthDays === 1) totalNextMonthStartDays = 6;
    else if (lastDayofMonthDays === 2) totalNextMonthStartDays = 5;
    else if (lastDayofMonthDays === 3) totalNextMonthStartDays = 4;
    else if (lastDayofMonthDays === 4) totalNextMonthStartDays = 3;
    else if (lastDayofMonthDays === 5) totalNextMonthStartDays = 2;
    else if (lastDayofMonthDays === 6) totalNextMonthStartDays = 1;
    else totalNextMonthStartDays = 0;

    const totalDays =
      date.daysInMonth() + totalLastMonthFinalDays + totalNextMonthStartDays;
    const monthList: Array<any> = Array.from({ length: totalDays });
    let counter = 1;

    for (let i = totalLastMonthFinalDays; i < totalDays; i++) {
      if (i < totalDays - totalNextMonthStartDays)
        monthList[i] = {
          no: counter,
          date: date.clone().startOf("month").add(counter - 1, "days"),
        };
      counter++;
    }

    setDaysGrid(monthList);
  };

  const changeMonth = (action: "add" | "subtract") => {
    if (action === "add") {
      setDate((prevDate) => prevDate.clone().add(1, "months"));
    } else if (action === "subtract") {
      setDate((prevDate) => prevDate.clone().subtract(1, "months"));
    }
  };

  return { date, changeMonth, daysGrid };
}