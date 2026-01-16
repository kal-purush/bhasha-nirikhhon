import { NextResponse } from 'next/server';

import { createClient } from '@/lib/supabase/server';

export async function POST(request: Request) {
  try {
    const { userId, songId } = await request.json();
    const supabase = await createClient();

    const { error } = await supabase
      .from('like_activities')
      .insert({ user_id: userId, song_id: songId });

    if (error) throw error;

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error('Error in search API:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to post like song' },
      { status: 500 },
    );
  }
}

export async function DELETE(request: Request) {
  try {
    const { userId, songId } = await request.json();
    const supabase = await createClient();

    const { error } = await supabase
      .from('like_activities')
      .delete()
      .match({ user_id: userId, song_id: songId });

    if (error) throw error;

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error('Error in search API:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to delete like song' },
      { status: 500 },
    );
  }
}