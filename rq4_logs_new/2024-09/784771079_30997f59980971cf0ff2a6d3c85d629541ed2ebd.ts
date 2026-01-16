import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/db';

export async function GET(request: NextRequest, { params }: { params: { userid: string } }) {
  const userId = params.userid;
  try {
    const user = await db
      .selectFrom('User')
      .selectAll()
      .where('User.id', '=', userId)
      .executeTakeFirst();

    if (!user) {
      return NextResponse.json({ error: 'User not found' }, { status: 404 });
    }

    const [skills, educations, experiences] = await Promise.all([
      db.selectFrom('Skill').selectAll().where('userId', '=', user.id).execute(),
      db.selectFrom('Education').selectAll().where('userId', '=', user.id).execute(),
      db.selectFrom('Experience').selectAll().where('userId', '=', user.id).execute(),
    ]);

    const userDetails = {
      user,
      skills,
      educations,
      experiences,
    };

    return NextResponse.json(userDetails);
  } catch (error) {
    console.error('Error fetching user profile:', error);
    return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
  }
}