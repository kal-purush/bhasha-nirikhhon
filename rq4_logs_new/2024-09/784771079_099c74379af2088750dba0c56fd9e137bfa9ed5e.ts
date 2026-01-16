import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/db';

export async function POST(req: NextRequest, { params }: { params: { userid: string } }) {
  const loggedInUserId = req.headers.get('X-User-Id');
  if (!loggedInUserId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 401 });
  }
  const userId = params.userid;
  const body = await req.json();
  const { feedback, rating } = body;

  if (req.method !== 'POST') {
    return NextResponse.json({ message: 'Method not allowed' }, { status: 405 });
  }

  if (!userId || !feedback || typeof rating !== 'number') {
    return NextResponse.json({ message: 'Invalid input' }, { status: 400 });
  }

  try {
    await db
      .insertInto('MentorFeedback')
      .values({
        mentorId: userId,
        menteeId: loggedInUserId,
        feedback: feedback,
        rating: rating,
      })
      .returningAll()
      .executeTakeFirstOrThrow();

    // Calculate the average rating
    const ratings = await db
      .selectFrom('MentorFeedback')
      .select('rating')
      .where('mentorId', '=', userId)
      .execute();
    const averageRating = ratings.reduce((acc, curr) => acc + curr.rating, 0) / ratings.length;
    const roundedAverageRating = Math.round(averageRating * 2) / 2;

    const updatedUser = await db
      .updateTable('User')
      .set('overallRating', roundedAverageRating)
      .where('id', '=', userId)
      .returningAll()
      .executeTakeFirstOrThrow();

    const response = {
      data: updatedUser,
      message: 'Feedback saved successfully and user rating updated',
    };

    return NextResponse.json(response, { status: 201 });
  } catch (error) {
    console.error('Error saving mentor feedback:', error);
    return NextResponse.json({ message: 'Failed to save mentor feedback' }, { status: 500 });
  }
}