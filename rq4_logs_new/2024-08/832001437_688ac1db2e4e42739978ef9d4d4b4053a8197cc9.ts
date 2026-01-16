import { NextResponse } from 'next/server';
import { auth } from '@clerk/nextjs/server';
import { db } from '@rocket-house-productions/integration';

export async function PUT(
  req: Request,
  { params }: { params: { courseId: string; moduleId: string; lessonId: string; childId: string } },
) {
  try {
    const { userId } = auth();
    const { isCompleted } = await req.json();

    if (!userId) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    // check if childId is enrolled on purchase of userId
    const child = await db.child.findFirst({
      where: {
        account: {
          userId: userId,
        },
        id: params.childId,
      },
    });

    if (!child) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    const userProgress = await db.childProgress.upsert({
      where: {
        childId_lessonId: {
          childId: child.id,
          lessonId: params.lessonId,
        },
      },
      update: {
        isCompleted: isCompleted,
      },
      create: {
        childId: child.id,
        lessonId: params.lessonId,
        isCompleted: isCompleted,
      },
    });

    return NextResponse.json(userProgress);
  } catch (error) {
    console.log('[COURSES_COURSE-ID_CHAPTERS_CHAPTER-ID_PROGRESS]', error);
    return new NextResponse('Internal Error', { status: 500 });
  }
}