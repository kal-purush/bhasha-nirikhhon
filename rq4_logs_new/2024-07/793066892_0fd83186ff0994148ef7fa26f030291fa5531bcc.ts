"use server";

import { getCookie } from "./cookie_actions";

export const getWeeklyReport = async (id:any) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/student/report/week?studentId=${id}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
        cache: "force-cache",
        next: {
          tags: ["weeklyReport"],
        },
      }
    );

    const data = await res.json();

    return data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(
        `Error in fetching student weekly report: ${error.message}`
      );
    } else {
      throw new Error(
        "An unknown error occurred while fetching student weekly report"
      );
    }
  }
};

export const getMonthlyReport = async (id:any) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/student/report/month?studentId=${id}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
        cache: "force-cache",
        next: {
          tags: ["monthlyReport"],
        },
      }
    );

    const data = await res.json();

    return data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(
        `Error in fetching student monthly report: ${error.message}`
      );
    } else {
      throw new Error(
        "An unknown error occurred while fetching student monthly report"
      );
    }
  }
};

export const getOverallReport = async (id:any) => {
  try {
    const token = await getCookie("token");

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_MENTOR_API_BASE_URL}/api/student/report/overall?studentId=${id}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Cookie: `token=${token}`,
        },
        credentials: "include",
        cache: "force-cache",
        next: {
          tags: ["overallReport"],
        },
      }
    );

    const data = await res.json();

    return data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(
        `Error in fetching student monthly report: ${error.message}`
      );
    } else {
      throw new Error(
        "An unknown error occurred while fetching student monthly report"
      );
    }
  }
};