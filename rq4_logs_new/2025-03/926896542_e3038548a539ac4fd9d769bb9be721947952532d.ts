import Categories from "@/app/_components/Categories";
import { PrismaClient } from "@prisma/client";
import { NextRequest, NextResponse } from "next/server";

const prisma = new PrismaClient();

// GET: /admin/posts/[id] 投稿記事を取得
export const GET = async (
  request: NextRequest,
  { params }: { params: { id: string } },
) => {
  // 分割代入でparamsからidを取得
  const { id } = params

  try {
    const post = await prisma.post.findUnique({
      where: {
        id: parseInt(id)
      },
      include: {
        postCategories: {
          include: {
            category: {
              select: {
                id: true,
                name: true,
              },
            },
          },
        },
      },
    })

    return NextResponse.json({ status: 'OK', post: post }, { status: 200 })
  } catch (error) {
    if (error instanceof Error) 
      return NextResponse.json({ status: error.message }, { status: 400 })
  }
}

// PUT: /admin/posts/[id] 管理者記事更新API
// 記事の更新時に送られてくるリクエストのbodyの型
type UpdatePostRequestBody = {
  title: string,
  content: string,
  categories: { id: number }[],
  thumbnailUrl: string,
}

// PUTという命名にすることで、PUTリクエストの時にこの関数が呼ばれる
// PUT requestの処理
export const PUT = async (
  request: NextRequest,
  { params }: { params: { id: string } }, // ここでリクエストパラメーターを受け取る
) => {
  // paramsの中にidが入っているのでそれを取り出す、分割代入
  const { id } = params

  // リクエストのbodyを取得
  const { title, content, categories, thumbnailUrl }: UpdatePostRequestBody = await request.json()

  try {
    // idを指定してPostを更新
    const updatedPost = await prisma.post.update({
      where: {
        id: Number(id) // parseInt(id)
      },
      data: {
        title,
        content,
        thumbnailUrl,
      },
    })

    // 一旦記事とカテゴリーの中間テーブルのレコードをすべて削除
    await prisma.postCategory.deleteMany({
      where: {
        postId: Number(id) // parseInt(id)
      },
    })

    // 記事とカテゴリーの中間テーブルのレコードをDBに生成
    // 本来複数同時生成には、createManyというメソッドがあるが、
    // sqliteではcreateManyが使えないので、for文1つずつ実施  
    for (const category of categories) {
      await prisma.postCategory.create({
        data: {
          postId: updatedPost.id,
          categoryId: category.id,
        },
      })
    }

    // レスポンスを返す
    return NextResponse.json({ status: 'OK', post: updatedPost }, { status: 200 })
  } catch (error) {
    if (error instanceof Error) {
      return NextResponse.json({ status: error.message }, { status: 400 })
    }
  }
}

// DELETE: /admin/posts 記事削除API

// DELETEという命名にすることで、DELETEリクエストの時にこの関数が呼ばれる
export const DELETE = async (
  request: NextRequest,
  { params }: { params: { id: string } },
) => {
  const { id } = params

  try {
    // idを指定してpostを削除
    await prisma.post.delete({
      where: {
        id: Number(id),
      },
    })

    // レスポンスを返す
    return NextResponse.json({ status: 'OK' }, { status: 200 })
  } catch (error) {
    if (error instanceof Error) 
      return NextResponse.json({ status: error.message }, { status: 400 })
  }
}