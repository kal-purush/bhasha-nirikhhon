import { NextResponse } from "next/server";

import prisma from "../../../../../prisma/client";


// DETAIL
export async function GET(request, { params }) {
    const id = params.id;
  
    const portof = await prisma.porto.findUnique({
      where: {
        id,
      },
    });
    if (!portof) {
      return NextResponse.json(
        {
          success: true,
          message: "Porto not found",
          data: null,
        },
        {
          status: 404,
        }
      );
    }
    return NextResponse.json(
      {
        success: true,
        message: "Porto posted",
        data: portof,
      },
      {
        status: 200,
      }
    );
  }
  
  
  // UPDATE
  export async function PATCH(request, { params }) {
    const id = params.id;
    const { title, content } = await request.json();
    const portof = await prisma.porto.update({
      where: {
        id,
      },
      data: {
        title: title,
        content: content,
        updatedAt: new Date(),
      },
    });
    return NextResponse.json(
      {
        sucess: true,
        message: "Data Post Updated!",
        data: portof,
      },
      {
        status: 200,
      }
    );
  }
  
  
  // DELETE
  export async function DELETE(request, {params}){
    const id = params.id;
  
    await prisma.porto.delete({
      where: {
        id,
      }
    });
  
    return NextResponse.json(
      {
        success: true,
        message: "Data Post Deleted"
      },
      {
        status: 200
      }
    )
  }