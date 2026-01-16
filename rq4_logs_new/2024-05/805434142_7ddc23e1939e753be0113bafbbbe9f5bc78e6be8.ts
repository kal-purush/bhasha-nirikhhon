"use server";
import { PrismaCli } from "../prismaCli/prismaClient";
import { revalidatePath } from "next/cache";

export const deleteCategoryAction = async (formData: any) => {
  const result = formData.map((item: any) => ({ id: item }));

  try {
    await PrismaCli.category.deleteMany({
      where: {
        OR: result,
      },
    });
    revalidatePath("/post");

    return {
      message: "Delete Category successfully",
    };
  } catch (err: any) {
    throw new Error("User Update failed");
  }
};