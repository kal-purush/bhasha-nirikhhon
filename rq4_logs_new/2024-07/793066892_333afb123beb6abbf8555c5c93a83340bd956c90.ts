"use server";

import { revalidateTag } from "next/cache";
import { getCookie } from "./cookie_actions";

export const getMeetings = async (studentId: string) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/meeting/get?studentId=${studentId}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
        cache: "force-cache",
        next: {
          tags: ["meetingsData"],
        },
      }
    );

    const data = await res.json();

    return data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(`Error in getting meetings: ${error.message}`);
    } else {
      throw new Error("An unknown error occurred while getting meetings");
    }
  }
};

export const acceptMeeting = async (meetingId: string) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/meeting/accept/${meetingId}`,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
      }
    );

    const data = await res.json();

    revalidateTag("meetingsData");

    return data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(`Error in accepting meetings: ${error.message}`);
    } else {
      throw new Error("An unknown error occurred while accepting meetings");
    }
  }
};

export const rescheduleMeeting = async (
  meetingId: string,
  data: { date: Date; time: string }
) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/meeting/reschedule/${meetingId}`,
      {
        method: "PUT",
        body: JSON.stringify(data),
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
      }
    );

    const responseData = await res.json();

    revalidateTag("meetingsData");

    return responseData;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(`Error in rescheduling meetings: ${error.message}`);
    } else {
      throw new Error("An unknown error occurred while rescheduling meetings");
    }
  }
};