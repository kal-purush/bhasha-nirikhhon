import { NextResponse, type NextRequest } from 'next/server';
import { type InsertDocument } from '@/db/types';
import { documentsTable } from '@/db/schema';
import { getUser } from '@/lib/auth';
import { db } from '@/db';
import { z } from 'zod';

const bodySchema = z.object({
  title: z.string().min(1, 'Title is required'),
});

export async function POST(req: NextRequest): Promise<NextResponse> {
  try {
    const user = await getUser();
    if (!user) {
      return NextResponse.json(
        { message: 'Unauthorized or failed to get user from Supabase Auth.' },
        { status: 401 },
      );
    }

    const body = await req.json();
    const parsed = bodySchema.safeParse(body);

    if (!parsed.success) {
      return NextResponse.json({ message: parsed.error.message }, { status: 400 });
    }

    const newDocument = await db
      .insert(documentsTable)
      .values({
        userId: user.id,
        title: parsed.data.title,
      } satisfies InsertDocument)
      .returning();

    return NextResponse.json({ id: newDocument[0].id }, { status: 201 });
  } catch (error) {
    console.error('Unknown error', error);
    return NextResponse.json(
      { message: 'Internal server error while creating new document' },
      { status: 500 },
    );
  }
}