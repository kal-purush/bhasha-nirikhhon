import { useMutation } from '@tanstack/react-query'
import { z } from 'zod'

import { API_URL } from '@/api'

import { inviteFormSchema } from './invite-form-schema'

export const useSendInvite = () => {
    const { mutate, error, isPending, isSuccess } = useMutation<
        undefined,
        Error,
        z.infer<typeof inviteFormSchema>
    >({
        mutationFn: async (data) => {
            const response = await fetch(API_URL, {
                method: 'post',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: data.name, email: data.email }),
            })

            if (!response.ok) {
                const answer = await response.json()
                throw new Error(answer.errorMessage)
            }
            return response.json()
        },
    })

    return {
        sendInvite: mutate,
        error,
        isPending,
        isSuccess,
    }
}