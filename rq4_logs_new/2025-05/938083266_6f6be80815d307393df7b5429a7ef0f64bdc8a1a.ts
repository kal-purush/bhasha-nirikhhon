import { menuAPI } from "../api";
import { MenuItem } from "../types";

const getMenu = async (firstCategory: string | number) => {
  try {
    const response = await fetch(menuAPI.find(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        firstCategory: firstCategory,
      }),
      next: {
        revalidate: 3600,
      },
    });

    if (!response.ok) {
      console.error(response);
      throw new Error("Произошла ошибка! Попробуйте позже");
    }

    const data: MenuItem[] = await response.json();
    return data;
  } catch (e) {
    console.error(e);
    throw e;
  }
};

export const menuHandler = {
  getMenu,
};