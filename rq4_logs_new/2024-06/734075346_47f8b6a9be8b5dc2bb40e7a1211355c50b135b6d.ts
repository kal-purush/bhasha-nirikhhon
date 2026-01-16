import { db } from "@/lib/db";
import { Category } from "@prisma/client";

export const getCategories = async () => {
  const categories = await db.category.findMany({
    orderBy: {
      title: "asc",
    },
  });
  return categories as Category[];
};