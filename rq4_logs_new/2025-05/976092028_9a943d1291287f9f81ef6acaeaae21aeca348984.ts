import { NextResponse, type NextRequest } from 'next/server';
import { documentsTable } from '@/db/schema';
import { getUser } from '@/lib/auth';
import { Document } from '@/types';
import { eq } from 'drizzle-orm';
import { db } from '@/db';
import { z } from 'zod';

const uuidSchema = z.string().uuid();

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
): Promise<NextResponse> {
  try {
    const user = await getUser();
    if (!user) {
      return NextResponse.json(
        { message: 'Unauthorized or failed to get user from Supabase Auth.' },
        { status: 401 },
      );
    }

    const { id } = await params;
    const parsed = uuidSchema.safeParse(id);

    if (!parsed.success) {
      return NextResponse.json({ message: parsed.error.message }, { status: 400 });
    }

    const [document] = await db
      .select()
      .from(documentsTable)
      .where(eq(documentsTable.id, parsed.data));

    if (!document || document.userId !== user.id) {
      return NextResponse.json(
        { message: 'Either the document is not found, or you are not allowed to access it.' },
        { status: 404 },
      );
    }

    return NextResponse.json({ document: document satisfies Document }, { status: 200 });
  } catch (error) {
    console.error('Unknown error', error);
    return NextResponse.json(
      { message: `Internal server error while fetching document with id: ` },
      { status: 500 },
    );
  }
}