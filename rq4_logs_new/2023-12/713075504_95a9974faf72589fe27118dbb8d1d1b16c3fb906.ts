import { TRPCError } from "@trpc/server";
import { hash } from "bcrypt";
import { and, eq, lte } from "drizzle-orm";
import { $t } from "~/i18n/dummy";
import { publicProcedure } from "~/server/api/trpc";
import { usersTable, verificationTokensTable } from "~/server/db/schema";
import { resetPasswordSchema } from "~/shared/auth/auth-schemas";

export const resetPassword = publicProcedure
  .input(resetPasswordSchema)
  .mutation(async ({ input, ctx }) => {
    return ctx.db.transaction(async (trx) => {
      /**
       * Look for user we are claiming to be
       */
      const [verificationToken] = await trx
        .select()
        .from(verificationTokensTable)
        .where(
          and(
            eq(verificationTokensTable.token, input.token),
            eq(verificationTokensTable.type, "reset-password"),
            lte(verificationTokensTable.expires, new Date()),
          ),
        );

      if (!verificationToken) {
        throw new TRPCError({
          code: "BAD_REQUEST",
          message: $t("auth.err.invalidToken"),
        });
      }

      const [existingUser] = await trx
        .select({ id: usersTable.id })
        .from(usersTable)
        .where(eq(usersTable.email, verificationToken.identifier));

      /**
       * This should never happen, we got the right token that somehow references to the wrong user.
       * We should just inform the user that the reset failed.
       */
      if (!existingUser) {
        throw new TRPCError({
          code: "INTERNAL_SERVER_ERROR",
          message: $t("auth.err.resetPasswordFailed"),
        });
      }

      const hashedPassword = await hash(input.password, 12);

      /**
       * Change the password
       */
      await trx
        .update(usersTable)
        .set({
          password: hashedPassword,
        })
        .where(eq(usersTable.id, existingUser.id));

      /**
       * Delete the token
       * We don't need it anymore
       * It's a one time use token
       */
      await trx
        .delete(verificationTokensTable)
        .where(
          and(
            eq(verificationTokensTable.token, input.token),
            eq(verificationTokensTable.type, "reset-password"),
          ),
        );

      return { success: true };
    });
  });