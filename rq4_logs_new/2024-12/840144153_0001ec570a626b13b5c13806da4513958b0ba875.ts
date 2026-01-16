"use server";
import { QuoteFormSchema, TQuoteForm } from "@/validators/quotes-form";
import { cookies } from "next/headers";
export const writeQuote = async (quote: TQuoteForm) => {
  const cookieStore = cookies();
  const token = cookieStore?.get("jwt")?.value;
  try {
    const validatedFields = QuoteFormSchema.safeParse(quote);

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
    const res = await fetch(`${process.env.NEXT_PUBLIC_STRAPI_URL}api/quotes`, {
      method: "POST",
      body: JSON.stringify({ data: quote }),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    });
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