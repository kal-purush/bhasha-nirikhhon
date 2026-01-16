import { createClient } from '@/lib/supabase/server-client';
import { ensureUserAvailabilityInDb } from '@/lib/auth';
import { NextResponse } from 'next/server';

export async function POST(): Promise<NextResponse> {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ message: 'Unauthorized' }, { status: 401 });
  }

  await ensureUserAvailabilityInDb({
    id: user.id,
    displayName: user.user_metadata['full_name'] as string,
    email: user.email!,
    emailConfirmed: user.user_metadata['email_verified'] as boolean,
    image: user.user_metadata['avatar_url'] || user.user_metadata['picture'],
    createdAt: new Date(user.created_at),
    updatedAt: new Date(user.updated_at || user.created_at),
  });

  return NextResponse.json(
    { message: 'User information updated to backend successfully' },
    { status: 200 },
  );
}