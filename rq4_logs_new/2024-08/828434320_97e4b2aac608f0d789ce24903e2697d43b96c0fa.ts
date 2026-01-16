import axios from "axios";

export function setAlarm(alarmAllow: boolean) {
  const BASE_URL = process.env.REACT_APP_BACK_URL;
  const data = `{\n    "isAlarm": "${alarmAllow}"\n}`;

  const config = {
    method: "patch",
    maxBodyLength: Infinity,
    url: `${BASE_URL}/users/edit/alarm`,
    headers: {
      Jwt: "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIzNjQ5NjU4MTQzIiwiaWF0IjoxNzIzMDg1ODA4LCJleHAiOjE3MjY2ODU4MDgsInVzZXJJZCI6MX0.0y4fkQBnXqIXNJEPt9RZRpCI0HDBCE50KOPeHjelCw8",
    },
    data: data,
  };

  axios(config)
    .then(function (response) {
      console.log(JSON.stringify(response.data));
    })
    .catch(function (error) {
      console.log(error);
    });
}