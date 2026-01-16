import { PrismaCli } from "../prismaCli/prismaClient";

export const fetchUser = async (filter: string, page?: string) => {
  const postsPerPage = Number(filter) || 2;
  const currentPage = Number(page) || 1;
  console.log(currentPage);

  const totalUsers = await PrismaCli.user.count();
  const skipUser = postsPerPage * (currentPage - 1);

  const totalPages = Math.round(totalUsers / postsPerPage);
  console.log(totalPages);

  try {
    const result = await PrismaCli.user.findMany({
      select: {
        id: true,
        username: true,
        email: true,
        status: true,
      },
      take: postsPerPage,
      skip: skipUser,
    });
    console.log(totalPages);
    return { result, totalPages };
  } catch (err: any) {
    throw new Error("Fetch User Failed");
  }
};