import { vi, describe, it, expect, beforeEach } from 'vitest'
import { rate } from '@/app/movie/lib/actions'
import { createClient } from '@/app/utils/supabase/server'

vi.mock('@/app/utils/supabase/server', () => ({
  createClient: vi.fn()
}))

vi.mock('next/cache', () => ({
  revalidatePath: vi.fn()
}))

vi.mock('next/navigation', () => ({
  redirect: vi.fn()
}))

describe('rate 함수', () => {
  let mockSupabase: any

  beforeEach(() => {
    mockSupabase = {
      from: vi.fn().mockReturnThis(),
      insert: vi.fn()
    }
    ;(createClient as any).mockReturnValue(mockSupabase)
  })

  it('유효한 입력으로 평점을 등록해야 합니다', async () => {
    const formData = new FormData()
    formData.append('movie_id', '123')
    formData.append('rating', '4')
    formData.append('user_id', 'user123')
    formData.append('the_movie_db_id', 'tmdb123')
    formData.append('category', 'horror')

    mockSupabase.insert.mockResolvedValue({ data: {}, error: null })

    await rate({}, formData)

    expect(mockSupabase.from).toHaveBeenCalledWith('rate')
    expect(mockSupabase.insert).toHaveBeenCalledWith([
      { rate_user_id: 'user123', rate_movie_id: 'tmdb123', score: 4 }
    ])
  })

  it('유효하지 않은 입력으로 오류를 반환해야 합니다', async () => {
    const formData = new FormData()
    formData.append('movie_id', '123')
    formData.append('rating', '6') // 유효하지 않은 평점
    formData.append('user_id', 'user123')
    formData.append('the_movie_db_id', 'tmdb123')
    formData.append('category', 'horror')

    const result = await rate({}, formData)

    expect(result.error).toBeDefined()
    expect(result.message).toBe('평점은 5 이하이어야 합니다')
  })

  it('Supabase 오류 시 오류를 반환해야 합니다', async () => {
    const formData = new FormData()
    formData.append('movie_id', '123')
    formData.append('rating', '4')
    formData.append('user_id', 'user123')
    formData.append('the_movie_db_id', 'tmdb123')
    formData.append('category', 'horror')

    mockSupabase.insert.mockResolvedValue({ data: null, error: { message: 'DB 오류' } })

    const result = await rate({}, formData)

    expect(result.error).toBe('DB 오류')
    expect(result.message).toBe('평점 등록 실패')
  })
})