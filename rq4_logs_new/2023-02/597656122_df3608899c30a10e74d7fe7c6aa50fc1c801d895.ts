import { useState } from "react";
import dayjs from "dayjs";

type Props = {
  lastUpdated: string;
};

export const useDate = () => {
  const formatDate = (props: Props) => {
    const { lastUpdated } = props;

    const cuerrentDate = dayjs();
    const lastUdated = dayjs(lastUpdated);

    const displayDate =
      cuerrentDate.format("YYYY/M/D") === lastUdated.format("YYYY/M/D")
        ? lastUdated.format("h:mm A")
        : lastUdated.format("M / D");

    return displayDate;
  };
  return { formatDate };
};