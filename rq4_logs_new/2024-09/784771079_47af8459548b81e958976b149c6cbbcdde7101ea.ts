import { db } from '@/lib/db';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const userId = req.headers.get('X-User-Id');
  if (!userId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 401 });
  }

  try {
    const unreadNotifications = await db
      .selectFrom('Notification')
      .selectAll()
      .where((eb) => eb.and([eb('userId', '=', userId), eb('read', '=', false)]))
      .execute();
    return NextResponse.json(unreadNotifications);
  } catch (error) {
    return NextResponse.json({ error: 'Failed to get user notifications' }, { status: 500 });
  }
}

export async function PUT(req: NextRequest) {
  const userId = req.headers.get('X-User-Id');
  if (!userId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 401 });
  }

  const { id, read } = await req.json();

  try {
    await db.updateTable('Notification').set({ read }).where('id', '=', id).execute();
    return NextResponse.json({ success: true });
  } catch (error) {
    return NextResponse.json({ error: 'Failed to update notification' }, { status: 500 });
  }
}