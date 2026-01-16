import axios from "axios";
const url = "http://localhost:5000";

export const addApplicant = async (applicantData) => {
  return await axios.post(`${url}/hostelApplicants`, applicantData);
};
axios.get();
axios.post();