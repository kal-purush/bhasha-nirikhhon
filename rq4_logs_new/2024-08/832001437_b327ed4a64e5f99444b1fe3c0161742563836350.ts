import { NextResponse } from 'next/server';
import { auth } from '@clerk/nextjs/server';
import { db } from '@rocket-house-productions/integration';

export async function PATCH(
  req: Request,
  {
    params,
  }: { params: { courseId: string; moduleId: string; lessonId: string; questionanaireId: string; answerId: string } },
) {
  try {
    const { userId } = auth();

    if (!userId) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    const course = await db.course.findUnique({
      where: {
        id: params.courseId,
      },
    });

    if (!course) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    const moduleSection = await db.module.findUnique({
      where: {
        id: params.moduleId,
        courseId: params.courseId,
      },
    });

    if (!moduleSection) {
      return new NextResponse('Not found', { status: 404 });
    }

    const lesson = await db.lesson.findUnique({
      where: {
        id: params.lessonId,
        moduleId: params.moduleId,
      },
    });

    if (!lesson) {
      return new NextResponse('Not found', { status: 404 });
    }

    const questionanaire = await db.questionary.findUnique({
      where: {
        id: params.answerId,
        lessonId: params.questionanaireId,
      },
    });

    if (!questionanaire) {
      return new NextResponse('Not found', { status: 404 });
    }

    const answer = await db.question.findUnique({
      where: {
        id: params.answerId,
        questionaryId: params.questionanaireId,
      },
    });

    if (!answer) {
      return new NextResponse('Not found', { status: 404 });
    }

    const publishedAnswer = await db.question.update({
      where: {
        id: params.answerId,
        questionaryId: params.questionanaireId,
      },
      data: {
        isPublished: true,
      },
    });

    return NextResponse.json(publishedAnswer);
  } catch (error) {
    console.log('[COURSES_COURSE-ID_MODDULE_LEDDONS-ID-QUESTION_PUBLISH]', error);
    return new NextResponse('Internal Error', { status: 500 });
  }
}