import http from 'k6/http';
import { check, sleep } from 'k6';


// Generoidaan 40 sekuntin ajan kuormitusta eri virtuaalikäyttäjien määrillä
export let options = {
  stages: [
    { duration: '10s', target: 100},
    { duration: '10s', target: 50},
    { duration: '10s', target: 25},
    { duration: '10s', target: 10},
  ],
};
export default function () {
  // __ENV.URLin avulla voidaan tuoda haluttu ip / dns osoite ulkopuolelta, esim. Azure DevOpsista
  let res = http.get(__ENV.URL);
  check(res, {'status was 200': (r) => r.status == 200});
  sleep(1);
}