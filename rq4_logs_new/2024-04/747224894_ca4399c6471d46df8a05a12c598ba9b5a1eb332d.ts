import { allGroupsExpensesRouter } from '~/server/api/routers/groups/allGroups/allGroupsExpenses';
import { allGroupsSettlementsRouter } from '~/server/api/routers/groups/allGroups/allGroupsSettlements';
import { createTRPCRouter, privateProcedure } from '~/server/api/trpc';

export const allGroupsRouter = createTRPCRouter({
  get: privateProcedure.query(async ({ ctx: { db, user } }) => {
    return db.group.findMany({
      where: {
        UserGroup: {
          some: {
            user,
          },
        },
      },
      include: {
        UserGroup: {
          include: {
            user: {
              select: {
                id: true,
                username: true,
                firstName: true,
                lastName: true,
                imageUrl: true,
              },
            },
          },
        },
      },
    });
  }),

  expenses: allGroupsExpensesRouter,
  settlements: allGroupsSettlementsRouter,
});