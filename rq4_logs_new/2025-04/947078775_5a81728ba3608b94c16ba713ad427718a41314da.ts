import { NextResponse } from 'next/server';

import { createClient } from '@/lib/supabase/server';
import { getAuthenticatedUser } from '@/utils/getAuthenticatedUser';

export async function POST(request: Request) {
  try {
    const supabase = await createClient(); // Supabase 클라이언트 생성
    const { songId } = await request.json();
    const userId = await getAuthenticatedUser(supabase); // userId 가져오기

    const { data, error } = await supabase.from('sing_logs').insert({
      song_id: songId,
      user_id: userId, // userId 추가
    });
    console.log(data);
    if (error) throw error;

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error('Error in tosings API:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to post sing_logs' },
      { status: 500 },
    );
  }
}