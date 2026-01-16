import Papa from "papaparse";

export const jsonToCsv = (data: string | object) => {
  try {
    let input: object[] = [];

    if (typeof data === "string") {
      input = JSON.parse(data);
    } else if (Array.isArray(data)) {
      input = data;
    } else if (typeof data === "object") {
      input = [data];
    } else {
      throw new Error("Invalid input");
    }

    const csv = Papa.unparse(input);
    return csv;
  } catch (error) {
    if (error instanceof Error) {
      return error.message;
    } else {
      throw new Error("An unexpected error occurred");
    }
  }
};