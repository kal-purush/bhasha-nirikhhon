"use server";

import prisma from "@/lib/prisma";
import { createHash } from "crypto";
import { nextGetServerSession } from "@/lib/next-auth";
import generateRandomSlug from "@/utils/randomSlug";
import { revalidatePath } from "next/cache";

export async function addLink(data: FormData) {
  const session = await nextGetServerSession();
  let hashedpass;
  let slug = data.get("slug") as string;
  try {
    if (data.get("password") as string) {
      hashedpass = createHash("md5")
        .update(data.get("password") as string)
        .digest("hex");
    }
    if (!slug) slug = generateRandomSlug();

    await prisma.link_Shortener.create({
      data: {
        slug,
        target_url: data.get("destLink") as string,
        password: hashedpass ? hashedpass : undefined,
        user_id: session?.user?.id!,
      },
    });
    revalidatePath("/admin/link");
    return { message: "Berhasil menambahkan link!", error: false };
  } catch (e) {
    console.error(e);
    return { message: "Gagal menambahkan link!", error: true };
  }
}
// omaigat ges ada ahsan