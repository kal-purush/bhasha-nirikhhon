"use server";

import prisma from "@/lib/prisma";
import { revalidatePath } from "next/cache";
import { z } from "zod";

const OddsTypeEnum = z.enum(["money-line", "totals", "pointspread"]);

const updateAdminSelectedSchema = z.object({
  id: z.string().uuid(),
  adminSelected: z.string().nullable(),
  oddsType: OddsTypeEnum.nullable(),
  drawTeam: z.string().nullable(),
});

export async function adminUseGame(formData: FormData) {
  try {
    const validation = updateAdminSelectedSchema.safeParse({
      id: formData.get("id"),
      adminSelected: formData.get("adminSelected"),
      oddsType: formData.get("oddsType"),
      drawTeam: formData.get("drawTeam"),
    });

    if (!validation.success) {
      console.log(validation.error);
      return { message: "Validation error" };
    }

    const { id, adminSelected, oddsType, drawTeam } = validation.data;

    if (!oddsType && !drawTeam) {
      throw new Error("Odds type and draw team are both null");
    }

    await prisma.matchups.update({
      where: {
        id: id,
      },
      data: {
        adminSelected: !!adminSelected,
        ...(oddsType ? { oddsType } : null),
        ...(drawTeam ? { drawTeam } : null),
      },
    });

    console.log({ id, adminSelected: !!adminSelected, oddsType, drawTeam });
    revalidatePath("/admin/matchups/picker");
    return { message: "Success. Matchup added" };
  } catch (error) {
    console.log(error);
    return { error, message: "Error selecting matchup" };
  }
}