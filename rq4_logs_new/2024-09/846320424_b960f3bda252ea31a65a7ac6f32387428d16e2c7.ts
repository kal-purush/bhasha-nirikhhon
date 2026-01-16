'use server'

import { revalidatePath } from 'next/cache'
import { redirect } from 'next/navigation'
import { z } from 'zod';
import { createClient } from '@/app/utils/supabase/server'

const rateSchema = z.object({
    rating: z.number().min(0, "평점은 0 이상이어야 합니다").max(5, "평점은 5 이하이어야 합니다"),
    user_id: z.string(),
    movie_id: z.string(),
    category: z.string(),
})

export type RateState = {
    error?: {
        rating?: string[]
        user_id?: string[]
        movie_id?: string[]
        category?: string[]
    }
    message?: string
}

export async function rate(prevState: RateState, formData: FormData) {
    const supabase = createClient()

    const validation = rateSchema.safeParse({
        rating: Number(formData.get('rating')) as number,
        user_id: formData.get('user_id') as string,
        movie_id: formData.get('movie_id') as string,
        category: formData.get('category') as string,
    })

    if (!validation.success) {
        return {
            error: validation.error.flatten().fieldErrors,
            message: "평점 등록 실패"
        }
    }
    const { user_id, movie_id, category, rating } = validation.data
    const { data, error } = await supabase.from('rate').insert([{rate_user_id: user_id, rate_movie_id: movie_id, score: rating}])
    if (error) {
        console.log(error)
        return {
            error: error.message,
            message: "평점 등록 실패"
        }
    }
    const url = `/movie/${movie_id}/${category}`
    revalidatePath(url)
    redirect(url)
}