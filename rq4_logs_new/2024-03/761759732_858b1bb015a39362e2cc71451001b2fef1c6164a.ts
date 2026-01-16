'use server'

import prisma from '@/lib/db'
import { revalidatePath } from 'next/cache'

export async function submitForm(formData: FormData) {

    const result = await prisma.user.update({
        where: {
            id: formData.get('userId')?.toString()
        },
        data: {
            name: formData.get('name')?.toString(),
            imgURL: formData.get('imgURL')?.toString(),
            intro: formData.get('intro')?.toString(),
            about: formData.get('markdown')?.toString()
        }
    })

    revalidatePath('/')
    revalidatePath('/about')

    console.log(result)
}