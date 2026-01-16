import axios, { AxiosRequestConfig, AxiosResponse } from "axios";
import dayjs from "dayjs";
import { periodName } from "../../pages/HealthChart/HealthChart";
import {
  dayResponseDataType,
  notDayResponseDataType,
} from "../../pages/HealthChart/dataTypes";

export const getChartData = async (period: periodName, today: dayjs.Dayjs) => {
  var config: AxiosRequestConfig = {
    method: "get",
    maxBodyLength: Infinity,
    url: `http://3.39.60.18:9000/diagnosis/${period}/${today.format(
      "YYYY-MM-DD"
    )}`,
    headers: {
      Jwt: "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIzNjQ5NjU4MTQzIiwiaWF0IjoxNzIzMDg1ODA4LCJleHAiOjE3MjY2ODU4MDgsInVzZXJJZCI6MX0.0y4fkQBnXqIXNJEPt9RZRpCI0HDBCE50KOPeHjelCw8",
    },
  };

  console.log(
    `http://3.39.60.18:9000/diagnosis/${period}/${today.format("YYYY-MM-DD")}`
  );

  try {
    const response: AxiosResponse<
      dayResponseDataType | notDayResponseDataType
    > = await axios(config);
    return response.data.result;
  } catch (error) {
    console.error(error);
    return null;
  }

  //   axios(config)
  //     .then(function (response) {
  //       console.log(JSON.stringify(response.data));
  //     })
  //     .catch(function (error) {
  //       console.log(error.response);
  //     });
};