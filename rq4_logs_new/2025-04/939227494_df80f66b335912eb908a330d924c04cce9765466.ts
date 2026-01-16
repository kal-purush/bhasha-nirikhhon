import { auth } from "@/auth";
import { DynamicProps } from "@/constants";
import { prisma } from "@/lib";
import { NextRequest, NextResponse } from "next/server";

export async function POST(req: NextRequest, { params }: DynamicProps<"id">) {
  try {
    const productId = (await params).id;

    const session = await auth();
    if (!session) {
      return NextResponse.json(
        {
          message: "you Should be login",
        },
        { status: 400 }
      );
    }

    const userId = session.user.id;

    const user = await prisma.user.findUnique({
      where:{
        id:userId
      },
      include:{
        favoriteItems:true
      }
    })

    const didLikeItBefore = user?.favoriteItems.some((item)=>item.id === productId)

    if(didLikeItBefore){
      await prisma.user.update({
        where: {
          id: userId,
        },
        data: {
          favoriteItems: {
            disconnect:{id:productId}
          },
        },
      });
      
    return NextResponse.json(
      {
        message: "The Product was Deleted from  to user's favorties",
        isInsideFavorties:false,
      },
      { status: 200 }
    );
    }else{
      await prisma.user.update({
        where: {
          id: userId,
        },
        data: {
          favoriteItems: {
            connect: { id: productId },
          },
        },
      });
      
    return NextResponse.json(
      {
        message: "The Product has added to user's favorties",        
        isInsideFavorties:true,

      },
      { status: 200 }
    );
    }

   

  } catch (error) {
    return NextResponse.json(
      {
        message: error,
      },
      { status: 500 }
    );
  }
}

