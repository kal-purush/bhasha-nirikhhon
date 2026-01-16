import { resources } from "@/dump/resources";

export const getResources = async (page: string | undefined) => {
  if (page) {
    return resources.filter((res) => res.page === page);
  }

  return resources;
};