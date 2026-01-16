"use server";
import { errorMsg } from "@/lib/utils";
import {
  AdminInquiryFormSchema,
  TAdminInquiryForm,
} from "@/validators/admin-inquiry-form";
import { cookies } from "next/headers";
export const addAdminInquiry = async (inquiry: TAdminInquiryForm) => {
  const cookieStore = cookies();
  const token = cookieStore?.get("jwt")?.value;
  if (!token) {
    throw new Error("You are not authorized or logged in");
  }
  try {
    const validatedFields = AdminInquiryFormSchema.safeParse(inquiry);

    if (!validatedFields.success) {
      return {
        error: {
          message:
            validatedFields?.error?.errors?.[0]?.message ||
            "Please check your input fields!",
        },
        status: 400,
      };
    }
    const res = await fetch(
      `${process.env.NEXT_PUBLIC_STRAPI_URL}api/admin-inquiries`,
      {
        method: "POST",
        body: JSON.stringify({ data: inquiry }),
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
      },
    );
    if (!res.ok) {
      const errorData = await res.json();
      throw {
        status: res.status,
        message: errorData.message || "An error occurred",
      };
    }
    const data = await res.json();

    return {
      data,
      status: res.status,
    };
  } catch (error: any) {
    console.log(error);
    return {
      error: {
        // @ts-ignore
        message: errorMsg(error.status, "Couldn't submit message"),
      },
      status: error?.response?.status || 500,
    };
  }
};