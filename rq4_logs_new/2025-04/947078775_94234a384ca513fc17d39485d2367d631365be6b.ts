import { NextResponse } from 'next/server';

import createClient from '@/lib/supabase/server';
import { getAuthenticatedUser } from '@/utils/getAuthenticatedUser';

export async function DELETE(request: Request) {
  try {
    const supabase = await createClient(); // Supabase 클라이언트 생성
    const userId = await getAuthenticatedUser(supabase); // userId 가져오기

    const { songIds } = await request.json();

    const { error } = await supabase
      .from('like_activities')
      .delete()
      .eq('user_id', userId)
      .in('song_id', songIds);

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