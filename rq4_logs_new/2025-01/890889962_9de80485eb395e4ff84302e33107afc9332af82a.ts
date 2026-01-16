import { prisma } from "@/lib/prisma";
import { NextRequest } from "next/server"
import type { User } from "@/services/type";

// ユーザー新規登録
export const POST = async(req: NextRequest) => {
  const reqData: Omit<User, 'id'|'createAt'|'book[]'> = await req.json();
  const existingUser = await prisma.user.findFirst({
    where: {
      email: reqData.email,
    },
  });

  if(existingUser) {
    // ユーザーが登録済み: null返す。
    return Response.json({
      user: null
    });
  }

  try {
    console.log("save user info start");
    await prisma.user.create({
      data: {
        name: reqData.name,
        email: reqData.email,
        password: reqData.password,
      }
    });
    console.log("save user info end");

    console.log(" get user info start");
    const user: User|null  = await prisma.user.findFirst({
      where: {
        email: reqData.email
      }
    })
    console.log(" get user info end");

    return Response.json(
      { user: user ?? null }
    );
  } catch (err) {
    console.log("save-failed");
    return Response.json(
      { message: "Internal Server Error" }, 
      { status: 500 }
    );
  }
}