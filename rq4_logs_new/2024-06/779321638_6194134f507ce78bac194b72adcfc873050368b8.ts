import { IBalanceExport } from "../../types/balance";
import { getToken } from "../../utilities";
import { BalanceExportEndpoint } from "../subscriptions/subscriptions.endpoints";

export const handleBalanceExport = async ({
  startDate,
  endDate,
  filename = "balance-export",
}: IBalanceExport) => {
  const token = getToken();

  const headers = {
    ...(token && { Authorization: `Bearer ${token}` }),
    "Content-Type": "application/json",
  };

  const response = await fetch(BalanceExportEndpoint({ startDate, endDate }), {
    method: "get",
    headers,
  });

  const fileBlob = await response.blob();
  const downloadUrl = URL.createObjectURL(fileBlob);
  const link = document.createElement("a");
  link.href = downloadUrl;
  link.download = filename;
  link.click();

  URL.revokeObjectURL(downloadUrl);
};