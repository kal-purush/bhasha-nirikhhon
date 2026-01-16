import { NextResponse } from 'next/server';

import createClient from '@/lib/supabase/server';
import { getAuthenticatedUser } from '@/utils/getAuthenticatedUser';

export async function POST(request: Request) {
  try {
    const supabase = await createClient(); // Supabase 클라이언트 생성
    const userId = await getAuthenticatedUser(supabase); // userId 가져오기
    const { songIds } = await request.json();

    const { data: maxRow, error: maxError } = await supabase
      .from('tosings')
      .select('order_weight')
      .eq('user_id', userId)
      .order('order_weight', { ascending: false })
      .limit(1);

    if (maxError) throw maxError;

    const lastWeight = maxRow?.[0]?.order_weight ?? 0;
    const newWeight = lastWeight + 1;

    const { error } = await supabase.from('tosings').insert(
      songIds.map((songId: string) => ({
        user_id: userId,
        song_id: songId,
        order_weight: newWeight,
      })),
    );

    if (error) throw error;

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error('Error in tosings API:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to post tosings song' },
      { status: 500 },
    );
  }
}