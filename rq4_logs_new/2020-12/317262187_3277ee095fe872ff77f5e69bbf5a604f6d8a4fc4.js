import flatpickr from 'flatpickr'
import 'flatpickr/dist/flatpickr.min.css'
import rangePlugin from "flatpickr/dist/plugins/rangePlugin";

const initDatePicker = () => {
  const dateElement = document.querySelector("#start_date")
  if (dateElement) {
    flatpickr("#start_date", {
      altInput: true,
      plugins: [new rangePlugin({input: "#end_date"})]
    });
  }
}
 export {initDatePicker};