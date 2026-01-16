'use server';

import { loginWithEmailAndPassword } from '@/lib/auth-client';
import { revalidatePath } from 'next/cache';
import { redirect } from 'next/navigation';

export async function loginAction(formData: FormData) {
  let redirectPath: string | null = null;

  const email = formData.get('email') as string;
  const password = formData.get('password') as string;
  try {
    await loginWithEmailAndPassword(email, password);
    revalidatePath('/', 'layout');
    redirectPath = '/';
  } catch (err) {
    console.error('Login Error', err);
    redirect('/error');
  } finally {
    if (redirectPath) redirect(redirectPath);
  }
}