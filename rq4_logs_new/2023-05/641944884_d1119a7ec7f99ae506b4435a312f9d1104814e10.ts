import axios from "axios"
import { InternshipStage, Period } from "./types"

const testData = {
  requests: {
    start: "2022-01-01",
    end: "2022-01-31",
  },
  studying: {
    start: "2022-02-01",
    end: "2022-02-28",
  },
  testing: {
    start: "2022-03-01",
    end: "2022-03-31",
  },
  hackathon: {
    start: "2022-04-01",
    end: "2022-04-30",
  },
  assignment: {
    start: "2022-05-01",
    end: "2022-05-31",
  },
  internship1: {
    start: "2022-06-01",
    end: "2022-06-30",
  },
  internship2: {
    start: "2022-07-01",
    end: "2022-07-31",
  },
  internship3: {
    start: "2022-08-01",
    end: "2022-08-31",
  },
}

type QueryResponse = Record<InternshipStage, Period>

export const fetchInternshipSchedule = async () => {
  return axios
    .get("https://api.publicapis.org/entries")
    .then((res) => testData as QueryResponse)
}