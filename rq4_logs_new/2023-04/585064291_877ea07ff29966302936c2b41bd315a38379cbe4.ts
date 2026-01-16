import axios from "axios";

export type BHourType = {
  weeks: string;
  hours: string;
};

type RequestDataType = {
  name: string;
  phoneNumber: string;
  newAddress: string;
  sido: string;
  sigungu: string;
  bname1: string;
  bname2: string;
  detailedAddress: string;
  comments: string;
  categoryType: string;
  registerNumber: string;
  registerName: string;
  tags: string[];
  businessHours: BHourType[];
};

export const createDangolStore = async (requestData: RequestDataType) => {
  const queryKey = "/api/v1/store/create?_csrf=957d8df2-0107-4a0e-b71e-faff75331ee0";

  const accessToken = localStorage.getItem("accessToken");
  const response = await axios.post(queryKey, requestData, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  return response.data;
};