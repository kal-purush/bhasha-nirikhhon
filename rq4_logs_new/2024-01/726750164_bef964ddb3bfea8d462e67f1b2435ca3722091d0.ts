import api from "@utils/axios";
import { CompanyInfo } from "@data/interface";
import { getAccessToken } from "@utils/authUtils";
export default async function recruiterInfoLoader(): Promise<{
  companyInfo: CompanyInfo | [];
} | null> {
  try {
    const res = await api.get("/companies/profile", {
      headers: { Authorization: `Bearer ${getAccessToken()}` },
    });

    const companyInfo = res.data.data.company as CompanyInfo;
    return { companyInfo };
  } catch (err) {
    return null;
  }
}