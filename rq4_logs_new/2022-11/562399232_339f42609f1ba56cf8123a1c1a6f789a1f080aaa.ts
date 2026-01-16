import { useRecoilValue } from "recoil";
import { useState } from "react";
import { dateAtom } from "@/lib/state/date";
import dayjs from "dayjs";
import isBetween from "dayjs/plugin/isBetween";
import useGetDaily from "./useGetDaily";
import { IDaily } from "../../../lib/state/interface";
dayjs.extend(isBetween);

const useDashAdData = () => {
  const [currentList, setCurrentList] = useState<IDaily[]>();
  const [prevList, setPrevList] = useState<IDaily[]>();

  const { daily } = useGetDaily();
  const date = useRecoilValue(dateAtom);
  const [startDate, endDate] = date;

  const getListByPeriod = (start: string, end: string) => {
    const dataListByPeriod = daily?.report.daily.filter((item) =>
      dayjs(item.date).isBetween(start, end, "day")
    );
    return dataListByPeriod;
  };

  const getCurrentDataList = () => {
    const filterd = getListByPeriod(startDate, endDate);
    setCurrentList(filterd);
  };

  const getPrevDataList = () => {
    const prevEnd = dayjs(endDate).subtract(1, "d").format("YYYY-MM-DD");
    const diff = dayjs(endDate).diff(startDate);

    const prevStart = dayjs(prevEnd)
      .subtract(diff, "millisecond")
      .format("YYYY-MM-DD");
    const filtered = getListByPeriod(prevStart, prevEnd);
    setPrevList(filtered);
  };

  return { currentList, prevList, getCurrentDataList, getPrevDataList };
};

export default useDashAdData;