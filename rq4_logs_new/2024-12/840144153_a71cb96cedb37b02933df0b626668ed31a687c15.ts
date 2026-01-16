"use server";

import axios, { AxiosResponse, type AxiosError } from "axios";
import { revalidateTag } from "next/cache";
import { cookies } from "next/headers";
export const deleteServiceRequest = async (id: number) => {
  const cookieStore = cookies();

  try {
    const res: AxiosResponse = await axios(
      `${process.env.NEXT_PUBLIC_STRAPI_URL}api/service-requests/${id}`,
      {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${cookieStore.get("jwt")?.value}`,
        },
      },
    );
    revalidateTag("service-requests");

    if (res.status !== 200) {
      throw new Error(res.statusText);
    }
    return res;
  } catch (error: AxiosError | any) {
    return {
      error: error?.response?.data || { message: "An error occurred!" },
      status: error?.response?.status || 500,
    };
  }
};