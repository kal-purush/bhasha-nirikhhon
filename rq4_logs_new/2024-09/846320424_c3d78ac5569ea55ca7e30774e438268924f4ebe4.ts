import { vi, describe, it, expect, beforeEach } from 'vitest'
import { signup, SignupState } from '@/app/auth/lib/actions'
import { createClient } from '@/app/utils/supabase/server'
import { redirect } from 'next/navigation'
import { revalidatePath } from 'next/cache'

// Supabase 클라이언트 모킹
vi.mock('@/app/utils/supabase/server', () => ({
    createClient: vi.fn(() => ({
      auth: {
        signUp: vi.fn(),
      },
    })),
  }))


  // Next.js 함수 모킹
vi.mock('next/navigation', () => ({
    redirect: vi.fn(),
  }))

vi.mock('next/cache', () => ({
    revalidatePath: vi.fn(),
  }))

describe('signup', () => {
  let prevState: SignupState
  let formData: FormData

  beforeEach(() => {
    prevState = {
      error: undefined,
      message: undefined,
    }
    formData = new FormData()
    formData.append('email', 'test@example.com')
    formData.append('password', 'Password123!')
    formData.append('name', 'Test User')

    vi.clearAllMocks()
  })

  it('성공적으로 회원가입을 수행해야 합니다', async () => {
    const mockSignUp = vi.fn().mockResolvedValue({ error: null })
    vi.mocked(createClient).mockReturnValue({
      auth: {
        signUp: mockSignUp,
      },
    } as any)

    await signup(prevState, formData)

    expect(mockSignUp).toHaveBeenCalledWith({
      email: 'test@example.com',
      password: 'Password123!',
      options: {
        data: {
          name: 'Test User',
        },
      },
    })
    expect(revalidatePath).toHaveBeenCalledWith('/', 'layout')
    expect(redirect).toHaveBeenCalledWith('/')
  })

  it('유효성 검사 실패 시(이메일 형식 올바르지 않음) 에러를 반환해야 합니다', async () => {
    formData.set('email', 'invalid-email')

    const result = await signup(prevState, formData)

    expect(result).toEqual({
      error: expect.objectContaining({
        email: expect.arrayContaining(['이메일 형식이 올바르지 않습니다']),
      }),
      message: "회원가입 실패",
    })
  })

  it('유효성 검사 실패 시(비밀번호 최소 8자 이상이어야 합니다) 에러를 반환해야 합니다', async () => {
    formData.set('password', 'short')

    const result = await signup(prevState, formData)

    expect(result).toEqual({
      error: expect.objectContaining({
        password: expect.arrayContaining(['비밀번호는 최소 8자 이상이어야 합니다']),
      }),
      message: "회원가입 실패",
    })
  })

  it('Supabase 에러 발생 시(이메일 중복) 리다이렉트해야 합니다', async () => {
    const mockSignUp = vi.fn().mockResolvedValue({ error: new Error('Supabase error') })
    vi.mocked(createClient).mockReturnValue({
      auth: {
        signUp: mockSignUp,
      },
    } as any)

    await signup(prevState, formData)

    expect(redirect).toHaveBeenCalledWith('/signup')
  })
})