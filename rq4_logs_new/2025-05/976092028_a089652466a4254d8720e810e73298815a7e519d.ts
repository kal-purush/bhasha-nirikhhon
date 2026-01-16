import { createClient } from '@/lib/supabase/server-client';
import { NextResponse } from 'next/server';
import { usersTable } from '@/db/schema';
import { type User } from '@/types';
import { eq } from 'drizzle-orm';
import { db } from '@/db';

export async function GET(): Promise<NextResponse> {
  const supabase = await createClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user) {
    return NextResponse.json(
      { message: 'Unauthorized or failed to get user from Supabase Auth.' },
      { status: 401 },
    );
  }

  try {
    const currentUser = await db
      .select()
      .from(usersTable)
      .where(eq(usersTable.id, user.id))
      .limit(1);
    if (!currentUser.length) {
      return NextResponse.json({ message: 'User not found' }, { status: 404 });
    }

    return NextResponse.json({ user: currentUser[0] satisfies User }, { status: 200 });
  } catch (error) {
    console.error('Error fetching user', error);
    return NextResponse.json(
      { message: 'Internal server error while fetching user' },
      { status: 500 },
    );
  }
}