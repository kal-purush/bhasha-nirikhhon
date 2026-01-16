import axios from "axios";

type addSubsCouponType = {
  type: "COUNT" | "MONTHLY";
  name: string;
  price: number;
  storeId: number;
  intro: string;
  isTop: boolean;
  useCount: number;
};

export const addSubsCoupon = async ({
  type,
  name,
  price,
  storeId,
  intro,
  isTop,
  useCount,
}: addSubsCouponType) => {
  const queryKey = "/api/v1/subscribe?_csrf=d3235088-26f1-419e-815b-09a208b2f8b8";
  const response = await axios.post(queryKey, {
    type,
    name,
    price,
    storeId,
    intro,
    isTop,
    useCount,
    benefits: [{ description: "" }],
  });
  return response.data;
};