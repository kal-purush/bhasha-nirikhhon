import { APIResponseCollection } from "@/types/types";

export async function getPackageCategories() {
  try {
    const searchParams = new URLSearchParams();
    //searchParams.append("populate", "*");
    searchParams.append("populate[packages][fields][0]","package_name")
    const res = await fetch(
      `${process.env.NEXT_PUBLIC_STRAPI_URL}api/package-categories?${searchParams.toString()}`,
    );
    if (!res.ok) {
      throw new Error("Error fetching package categories");
    }
    const data: APIResponseCollection<"api::package-category.package-category"> =
      await res.json();
    return data;
  } catch (error) {
    console.log(error);
  }
}