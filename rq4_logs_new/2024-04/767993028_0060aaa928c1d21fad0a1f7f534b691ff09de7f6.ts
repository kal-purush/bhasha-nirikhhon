import { Prisma } from "@prisma/client";
import prisma from "@/lib/prisma";

export const findAllLinks = async (
  filter?: Prisma.Link_ShortenerWhereInput,
) => {
  return prisma.link_Shortener.findMany({
    where: filter,
    orderBy: { created_at: "desc" },
    include: {
      user: { select: { name: true } },
      count: { select: { click_count: true } },
    },
  });
};


export const findLink = async (filter: Prisma.Link_ShortenerWhereInput) =>{
  return prisma.link_Shortener.findFirst({
    where: filter,
    include: {
      user: { select: { name: true } },
      count: { select: { click_count: true } },
    },
  });
}

export const createLink = async (data: Prisma.Link_ShortenerUncheckedCreateInput) =>{
  return prisma.link_Shortener.create({data})
}

export const updateLink = async (
  where: Prisma.Link_ShortenerWhereUniqueInput,
  update: Prisma.Link_ShortenerUncheckedUpdateInput,
) => {
  return await prisma.link_Shortener.update({ where, data: update });
};