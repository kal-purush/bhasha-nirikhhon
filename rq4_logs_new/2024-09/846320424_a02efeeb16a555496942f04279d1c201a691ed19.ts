'use server'

import { revalidatePath } from 'next/cache'
import { redirect } from 'next/navigation'
import { z } from 'zod';
import { createClient } from '@/app/utils/supabase/server'

const loginSchema = z.object({
  email: z.string().email("이메일 형식이 올바르지 않습니다"),
  password: z.string().min(8, "비밀번호는 최소 8자 이상이어야 합니다").regex(/^(?=.*[!@#$%^&*(),.?":{}|<>]).*$/, "비밀번호는 최소 하나의 특수문자를 포함해야 합니다"),})

const signupSchema = z.object({
  email: z.string().email("이메일 형식이 올바르지 않습니다"),
  password: z.string().min(8, "비밀번호는 최소 8자 이상이어야 합니다").regex(/^(?=.*[!@#$%^&*(),.?":{}|<>]).*$/, "비밀번호는 최소 하나의 특수문자를 포함해야 합니다"),
  name: z.string().min(1, "이름은 최소 1자 이상이어야 합니다"),
})

export async function login(formData: FormData) {
  const supabase = createClient()

  const validation = loginSchema.safeParse({
    email: formData.get('email') as string,
    password: formData.get('password') as string,
  })

  if (!validation.success) {
    return {
      error: validation.error.flatten().fieldErrors,
      message: "로그인 실패"
    }
  }
  const { email, password } = validation.data
  const data = {
    email,
    password,
  }

  const { error } = await supabase.auth.signInWithPassword(data)

  if (error) {
    redirect('/error')
  }

  revalidatePath('/', 'layout')
  redirect('/')
}

export async function signup(formData: FormData) {
    const validation = signupSchema.safeParse({
        email: formData.get('email') as string,
        password: formData.get('password') as string,
        name: formData.get('name') as string,
    })

    if (!validation.success) {
        return {
            error: validation.error.flatten().fieldErrors,
            message: "회원가입 실패"
        }
    }
    const supabase = createClient()
    const { email, password, name } = validation.data

  const { error } = await supabase.auth.signUp({
    email,
    password,
    options: {
        data: {
            name,
        },
    },
  })

  if (error) {
    redirect('/error')
  }

  revalidatePath('/', 'layout')
  redirect('/')
}