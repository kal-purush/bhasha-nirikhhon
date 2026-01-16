import { NextRequest, NextResponse } from 'next/server';
import { auth } from '@clerk/nextjs/server';
import { db } from '@rocket-house-productions/integration';

export async function POST(req: NextRequest) {
  try {
    const { userId } = auth();

    if (!userId) {
      return new NextResponse('Unauthorized operation', { status: 401 });
    }

    const data = await req.json();

    const score = await db.childScore.findUnique({
      where: {
        childId_courseId: {
          childId: data.childId,
          courseId: data.courseId,
        },
      },
    });

    let response = {};

    if (score) {
      /// update progress
      response = await db.childScore.update({
        where: {
          childId_courseId: {
            childId: data.childId,
            courseId: data.courseId,
          },
        },
        data: {
          score: data.score,
        },
      });
    } else {
      /// create progress
      response = await db.childScore.create({
        data: {
          childId: data.childId,
          courseId: data.courseId,
          score: data.score,
        },
      });
    }

    return new NextResponse(JSON.stringify(response), { status: 200 });
  } catch (error) {
    console.log('[COURSES PROGRESS]', error);
    return new NextResponse('Internal Error', { status: 500 });
  }
}