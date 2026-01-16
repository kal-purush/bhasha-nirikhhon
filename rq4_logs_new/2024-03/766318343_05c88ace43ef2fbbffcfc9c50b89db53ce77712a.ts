import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

const MAX_RECORDS = 50;
const MIN_OFFSET = 0;

export default {
  get: async ({ where = {}, orderBy = {}, limit = 10, offset = 0 } = {}) => {
    const take = Math.min(limit, MAX_RECORDS);
    const skip = Math.max(offset, MIN_OFFSET);

    return await prisma.article.findMany({
      where,
      orderBy,
      take,
      skip,
    });
  },
  count: async ({ where = {} } = {}) => {
    return await prisma.article.count({ where });
  },
};