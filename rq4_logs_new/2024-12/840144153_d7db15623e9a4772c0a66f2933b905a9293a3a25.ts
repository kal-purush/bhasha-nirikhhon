"use server";
import {
  BookAppointmentFormSchema,
  TBookAppointmentSchemaProvider,
} from "@/validators/book-appointment-validator";
import { cookies } from "next/headers";
export const makeAppointment = async (
  appointment: TBookAppointmentSchemaProvider,
) => {
  const cookieStore = cookies();
  const token = cookieStore?.get("jwt");
  try {
    const validatedFields = BookAppointmentFormSchema.safeParse(appointment);

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
      `${process.env.NEXT_PUBLIC_STRAPI_URL}api/appointments`,
      {
        method: "POST",
        body: JSON.stringify({ data: appointment }),
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
        message:
          error.status === 403
            ? "You are not authorized or logged in"
            : "Couldn't make an appointment. Please try again",
      },
      status: error?.response?.status || 500,
    };
  }
};