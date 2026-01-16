import { db } from "~/db/config.server";
import { invitations, lower, users } from "~/db/schema.server";
import { eq } from "drizzle-orm";
import type { FieldErrors } from "react-hook-form";
import { v7 as uuidv7 } from "uuid";
import {
  type CreateInvitationSchema,
  type DeleteInvitationSchema,
  INTENTS,
  type ResendInvitationSchema,
} from "~/routes/users/types";

export async function createInvitation(
  data: CreateInvitationSchema,
  receivedValues: Record<any, any>,
) {
  const lowercaseEmail = data.email.toLowerCase();

  try {
    return await db.transaction(async (tx) => {
      const existingUser = await tx
        .select()
        .from(users)
        .where(eq(lower(users.email), lowercaseEmail));
      if (existingUser.length) {
        const errors = {
          email: {
            type: "validate",
            message: "User already exists",
          },
        } as FieldErrors<CreateInvitationSchema>;
        return {
          errors,
          defaultValues: receivedValues,
        };
      }

      const existingInvitation = await tx
        .select()
        .from(invitations)
        .where(eq(lower(invitations.email), lowercaseEmail));
      if (existingInvitation.length) {
        const errors = {
          email: {
            type: "validate",
            message: "Invitation already exists",
          },
        } as FieldErrors<CreateInvitationSchema>;
        return {
          errors,
          defaultValues: receivedValues,
        };
      }

      const [insertedInvitation] = await tx
        .insert(invitations)
        .values({
          id: uuidv7(),
          email: lowercaseEmail,
        })
        .returning();

      await sendInvitationEmail(
        insertedInvitation.email,
        insertedInvitation.id,
      );

      return { success: true, intent: INTENTS.createInvitation };
    });
  } catch (e) {
    console.error("Error occurred while creating invitation", e);
    return { success: false, intent: INTENTS.createInvitation };
  }
}

export async function deleteInvitation(data: DeleteInvitationSchema) {
  try {
    await db.delete(invitations).where(eq(invitations.id, data.id));
    return { success: true, intent: INTENTS.deleteInvitation };
  } catch (e) {
    console.error("Error occurred while deleting invitation", e);
    return { success: false, intent: INTENTS.deleteInvitation };
  }
}

export async function resendInvitation(data: ResendInvitationSchema) {
  try {
    const invitation = await db.query.invitations.findFirst({
      where: eq(invitations.id, data.id),
    });
    if (!invitation) {
      return { success: false, message: `Invitation ${data.id} not found` };
    }
    await sendInvitationEmail(invitation.email, invitation.id);

    return { success: true, intent: INTENTS.resendInvitation };
  } catch (e) {
    console.error("Error occurred while resending invitation email", e);
    return { success: false, intent: INTENTS.resendInvitation };
  }
}

async function sendInvitationEmail(email: string, token: string) {
  console.log("sending email...", email, token);
}