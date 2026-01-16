"use server";

import { imageUploader } from "@/app/actions/fileUploader";
import prisma from "@/lib/prisma";
import { getServerSession } from "next-auth/next";
import { revalidatePath } from "next/cache";
import {
  updatePost,
  createPost,
  deletePost,
} from "@/utils/database/post.query";
import { MultiValue } from "react-select";

export async function upload(data: FormData) {
  let image = data.get("image") as File;
  const ABuffer = await image.arrayBuffer();

  try {
    const result = await imageUploader(Buffer.from(ABuffer));

    return {
      status: "OK",
      url: result.data?.url,
      message: "Link dicopy di clipboard!",
    };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal mengupload file!" };
  }
}

export async function createTag(inputValue: string) {
  try {
    await prisma.tag.create({ data: { tagName: inputValue } });
    revalidatePath("/admin/posts/create");
    return { status: "OK", message: "Tag berhasil dibuat!" };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal membuat tag!" };
  }
}

export async function postCreate(
  data: FormData,
  MD: string,
  tags: MultiValue<{ value: string; label: string }>,
) {
  const session = await getServerSession();
  session?.user?.user_pic;
  try {
    const title = data.get("title") as string;
    const slug = data.get("slug") as string;
    const description = data.get("desc") as string;
    const tag = tags.map((tag) => ({ tagName: tag.value }));
    const image = data.get("thumbnail") as File;
    const ABuffer = await image.arrayBuffer();
    const upload = await imageUploader(Buffer.from(ABuffer));
    delete session?.user?.image;

    await createPost({
      slug: slug,
      content: MD,
      title: title,
      description: description,
      thumbnail: upload.data?.url!,
      reaction: [],
      tags: { connect: tag },
      user: { connect: session?.user },
      created_at: new Date(),
      updated_at: new Date(),
      published: false,
    });
    revalidatePath("/berita");
    revalidatePath("/admin/posts");

    return { message: "Success" };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal menambahkan berita!" };
  }
}

export async function postUpdate(
  data: FormData,
  MD: string,
  tags: MultiValue<{ value: string; label: string }>,
  id: string,
) {
  const session = await getServerSession();
  try {
    const title = data.get("title") as string;
    const slug = data.get("slug") as string;
    const description = data.get("desc") as string;
    const tag = tags.map((tag) => ({ tagName: tag.value }));
    const image = data.get("thumbnail") as File;
    let upload;
    if (image) {
      const ABuffer = await image.arrayBuffer();
      upload = await imageUploader(Buffer.from(ABuffer));
    }

    delete session?.user?.image;

    await updatePost(
      { id: id },
      {
        slug: slug ?? undefined,
        content: MD ?? undefined,
        title: title ?? undefined,
        description: description ?? undefined,
        thumbnail: upload!.data?.url ?? undefined,
        tags: { connect: tag ?? undefined },
        updated_at: new Date(),
      },
    );
    revalidatePath("/berita");
    revalidatePath("/admin/posts");
    revalidatePath(`/admin/posts/${id}`);
    return { message: "Success" };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal menambahkan berita!" };
  }
}

export async function updatePostStatus(current_state: boolean, id: string) {
  try {
    await updatePost(
      { id: id },
      {
        published: !current_state,
        updated_at: new Date(),
        published_at: !current_state ? new Date() : undefined,
      },
    );
    revalidatePath("/berita");
    revalidatePath("/admin/posts");
    revalidatePath(`/admin/posts/${id}`);
    return { message: "Berhasil megupdate post!" };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal mengupdate post" };
  }
}

export async function postDelete(id: string) {
  try {
    await deletePost(id);
    revalidatePath("/berita");
    revalidatePath("/admin/posts");
    revalidatePath(`/admin/posts/${id}`);
    return { message: "Berhasil menghapus post!" };
  } catch (e) {
    console.log(e);
    return { error: true, message: "Gagal menghapus post" };
  }
}