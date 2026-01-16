import axios from "axios";

type editOwnerAccountType = {
  phoneNumber: string;
  marketingAgreement: boolean;
};

type postOwnerAccountType = {
  account: string;
  accountHolder: string;
  bank: string;
};

export const getOwnerAccount = async () => {
  const accessToken = localStorage.getItem("accessToken");
  const queryKey = "/api/v1/boss";
  const response = await axios.get(queryKey, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  return response.data;
};

export const editOwnerAccount = async ({
  phoneNumber,
  marketingAgreement,
}: editOwnerAccountType) => {
  const accessToken = localStorage.getItem("accessToken");
  const queryKey = "/api/v1/boss?_csrf=ff2dba70-94b7-45b8-b924-4f974757dbfe";
  const response = await axios.patch(
    queryKey,
    { phoneNumber, marketingAgreement },
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  return response.data;
};

export const postOwnerAccount = async ({ account, accountHolder, bank }: postOwnerAccountType) => {
  const accessToken = localStorage.getItem("accessToken");
  const queryKey = "/api/v1/boss/register-account?_csrf=a3e71c92-dc89-413b-b564-3c62d43f21de";
  const response = await axios.post(
    queryKey,
    { account, accountHolder, bank },
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  return response.data;
};