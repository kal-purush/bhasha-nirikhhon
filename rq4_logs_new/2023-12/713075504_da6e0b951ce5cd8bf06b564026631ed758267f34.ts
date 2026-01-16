import { TRPCError } from "@trpc/server";
import { and, eq } from "drizzle-orm";
import { z } from "zod";
import { $t } from "~/i18n/dummy";
import { withOrganizationAccess } from "~/server/api/organization/organization-queries";
import { protectedProcedure } from "~/server/api/trpc";
import { invoicesTable } from "~/server/db/schema";

export const invoiceDeleteById = protectedProcedure
  .input(z.object({ id: z.number(), organizationId: z.number() }))
  .mutation(async ({ ctx, input }) => {
    return ctx.db.transaction(async (trx) => {
      const [foundInvoice] = await withOrganizationAccess(
        trx
          .select({
            id: invoicesTable.id,
          })
          .from(invoicesTable)
          .where(and(eq(invoicesTable.id, input.id)))
          .limit(1)
          .$dynamic(),
        {
          column: invoicesTable.organizationId,
          userId: ctx.session.user.id,
          organizationId: input.organizationId,
        },
      );

      if (!foundInvoice) {
        throw new TRPCError({
          code: "NOT_FOUND",
          message: $t("invoice.err.notFound"),
        });
      }

      await trx
        .delete(invoicesTable)
        .where(eq(invoicesTable.id, foundInvoice.id));

      return { id: foundInvoice.id };
    });
  });