import { NextRequest, NextResponse } from "next/server";
import {prisma} from '@/lib/prisma'

export async function POST(request: NextRequest){
  const searchParams = request.nextUrl.searchParams
  const taskId = searchParams.get('task_id')
  if(!taskId) return;

  const deleteTask = prisma.tasks.delete({
    where: {
      id: +taskId
    },
  })
  const deleteAssignees = prisma.assignees.deleteMany({
    where: {
      taskId: +taskId
    }
  })
  try{
    await prisma.$transaction([deleteAssignees, deleteTask])

    return NextResponse.json({
      status: 'success',
    })
  }
  catch(err){
    return NextResponse.json({
      status: 'failure',
      message: err
    })
  }

}