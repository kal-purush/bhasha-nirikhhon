import { NextResponse } from 'next/server';
import { auth } from '@clerk/nextjs/server';
import { db } from '@rocket-house-productions/integration';

export async function DELETE(req: Request, { params }: { params: { courseId: string; awardId: string } }) {
  try {
    const { userId } = auth();

    if (!userId) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    const attachment = await db.awardType.delete({
      where: {
        id: params.awardId,
      },
    });

    return NextResponse.json(attachment);
  } catch (error) {
    console.log('[COURSES_COURSE-ID_DELETE_AWARD-ID]', error);
    return new NextResponse('Internal Error', { status: 500 });
  }
}

export async function PATCH(
  req: Request,
  {
    params,
  }: {
    params: { awardId: string };
  },
) {
  try {
    const { userId } = auth();
    const values = await req.json();

    if (!userId) {
      return new NextResponse('Unauthorized', { status: 401 });
    }

    const attachment = await db.awardType.update({
      where: {
        id: params.awardId,
      },
      data: {
        ...values,
      },
    });

    return NextResponse.json(attachment);
  } catch (error) {
    console.log('[COURSE_ID_MODULE_AWARD_AWARD_ID]', error);
    return new NextResponse('Internal Error', { status: 500 });
  }
}