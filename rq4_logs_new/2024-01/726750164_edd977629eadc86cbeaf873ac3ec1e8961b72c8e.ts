import { Option } from "@data/interface";
import api from "@utils/axios";
import { AxiosResponse } from "axios";

export default async function findJobPageLoader(): Promise<{
  positions: Option[];
  salaryRanges: Option[];
  fields: Option[];
  experienceRanges: Option[];
  workModes: Option[]; // Define the type for experienceRanges
} | null> {
  try {
    const [
      positionRes,
      salaryRes,
      fieldRes,
      experienceRes,
      workModeRes,
    ]: AxiosResponse[] = await Promise.all([
      api.get("/positions"),
      api.get("/salary-ranges"),
      api.get("/fields"),
      api.get("/experience-ranges"),
      api.get("/work-modes"),
    ]);

    // Process the responses here
    const positions = positionRes.data?.data?.positions || [];
    const salaryRanges = salaryRes.data?.data?.salary_ranges || [];
    const fields = fieldRes.data?.data?.fields || [];
    const experienceRanges = experienceRes.data?.data?.experience_ranges || [];
    const workModes = workModeRes.data?.data?.work_modes || [];

    return { positions, salaryRanges, fields, experienceRanges, workModes };
  } catch (err) {
    console.error("Error fetching data:", err);
    return {
      positions: [],
      salaryRanges: [],
      fields: [],
      experienceRanges: [],
      workModes: [],
    };
  }
}