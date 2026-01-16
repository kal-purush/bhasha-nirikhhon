import { AxiosRequestConfig } from "axios";
import { credicarAxiosInstance } from "@api/isettingCredicar";
import { IRequestPayrollAgre } from "@ptypes/payrollAgreement/RequestPayrollAgre/IRequestPayrollAgre/index.ts";
import { deleteWithRetries } from "@services/core/deleteWithRetries";
import { mapDeletePayrollnEntityToApi } from "./mappers";

const deletePayrollAgre = async (
  businessUnit: string,
  data: IRequestPayrollAgre,
): Promise<IRequestPayrollAgre> => {
  const config: AxiosRequestConfig = {
    headers: {
      "X-Action": "RemovePayrollForDeductionAgreement",
      "X-Business-Unit": businessUnit,
    },
  };

  const newData = await deleteWithRetries<IRequestPayrollAgre>(
    `/payroll-for-deduction-agreement`,
    config,
    mapDeletePayrollnEntityToApi(data) as unknown as string[],
    credicarAxiosInstance,
  );

  return newData;
};

export { deletePayrollAgre };