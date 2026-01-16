import { CompanyInfo, ListJobs } from "@data/interface";
import { AxiosResponse } from "axios";
import { LoaderFunctionArgs } from "react-router-dom";
import api from "@utils/axios";

export default async function companyDetailLoader(
  params: LoaderFunctionArgs
): Promise<{
  companyInfo: CompanyInfo | null;
  companyJobs: ListJobs | null;
} | null> {
  try {
    const [companyInfoRes, companyJobsRes]: AxiosResponse[] = await Promise.all(
      [
        api.get(`/companies/${params.params.id}`),
        api.get(`/companies/${params.params.id}/jobs`),
      ]
    );

    const companyInfo = companyInfoRes.data.data?.company;
    const companyJobs = companyJobsRes.data.data?.jobs;

    return { companyInfo, companyJobs };
  } catch (err) {
    return { companyInfo: null, companyJobs: null };
  }
}