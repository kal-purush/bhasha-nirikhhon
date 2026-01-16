"use server"

import { auth } from '@/lib/auth'
import { prisma } from '@/lib/prisma'
import { z } from 'zod'

const changeDescriptionSchema = z.object({
    description: z.string().min(4, "A descrição deve ter no mínimo 4 caracteres"),
})

type ChangeDescription = z.infer<typeof changeDescriptionSchema>

export async function changeDescription(data: ChangeDescription) {
    const session = await auth()
    const userId = session?.user.id

    if (!userId) {
        return {
            data: null,
            error: "Usuário não autenticado"
        }
    }


    const validation = changeDescriptionSchema.safeParse(data)

    if (!validation.success) {
        return {
            data: null,
            error: validation.error.issues[0].message
        }
    }

    try {
        const user = await prisma.user.update({
            where: {
                id: userId
            },
            data: {
                bio: data.description
            }
        })

        return {
            data: user.name,
            error: null
        }

    } catch (error) {
        console.error(error)
        return {
            data: null,
            error: 'Error ao salvar alterações'
        }
    }

}