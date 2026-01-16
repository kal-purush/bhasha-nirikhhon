import type { CustomSwellFile, SwellResponse } from "@/types/index";

import { getErrorMessage } from "@/utils/get-error-message";
import swell from "@/lib/server";

export const getS3Files = async (
  arr: string[],
): Promise<SwellResponse<CustomSwellFile> | string> => {
  // error if the array is empty or is not an array
  if (!Array.isArray(arr) || !arr.length) {
    throw new Error("Invalid S3 File IDs provided.");
  }

  if (arr.length > 100) {
    throw new Error(
      "Too many S3 File IDs provided. Please provide up to 10 ids.",
    );
  }

  try {
    return await swell.get(`/content/s-3-files`, {
      where: {
        id: { $in: arr },
      },
    });
  } catch (error) {
    return getErrorMessage(error);
  }
};