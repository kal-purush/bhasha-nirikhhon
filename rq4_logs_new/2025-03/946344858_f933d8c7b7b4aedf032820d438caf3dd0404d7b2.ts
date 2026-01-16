"use server";

import { postSchema } from "@/lib/zod-schemas";

export async function uploadPost(_: any, formData: FormData) {
  const data = {
    photo: formData.get("photo"),
    title: formData.get("title"),
    content: formData.get("content"),
  };

  const result = postSchema.safeParse(data);

  if (!result.success) {
    return result.error.flatten();
  }

  // data fetch
  console.log(result.data);
}