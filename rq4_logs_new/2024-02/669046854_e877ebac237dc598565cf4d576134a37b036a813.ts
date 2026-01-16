import { BASE_URL } from "@/constant/constants";

export function checkAuth() {
  return new Promise(async (resolve, reject) => {
    try {
      const response = await fetch(`${BASE_URL}/auth/check`, {
        credentials: "include", // Add this line
      });
      if (response.ok) {
        const data = await response.json();
        resolve({ data });
      } else {
        const error = await response.text();
        reject(error);
      }
    } catch (error) {
      reject(error);
    }
  });
}