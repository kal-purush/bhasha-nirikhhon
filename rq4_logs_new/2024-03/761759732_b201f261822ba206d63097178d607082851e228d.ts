'use server'

import prisma from '@/lib/db'
import { revalidatePath } from 'next/cache'
import { RedirectType, redirect } from 'next/navigation'

export async function updateUserInfo(formData: FormData) {

    await prisma.user.update({
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
    redirect('/about', RedirectType.replace)
}

export async function addCategory(formData: FormData) {
    if (formData.get('title') && formData.get('title'))
        await prisma.category.create({
            data: {
                title: formData.get('title')!.toString(),
                imgURL: formData.get('imgURL')!.toString(),
            }
        })
    revalidatePath('/portfolio')

}

export async function addArticle(formData: FormData) {

    await prisma.article.create({
        data: {
            title: formData.get('title')!.toString(),
            imgURL: formData.get('imgURL')!.toString(),
            type: formData.get('type')!.toString(),
            intro: formData.get('intro')!.toString(),
            brief: formData.get('brief')!.toString(),
            description: formData.get('markdown')!.toString(),
            categoryId: formData.get('categoryId') ? formData.get('categoryId')?.toString() : null
        }
    })
    if (formData.get('categoryId') && formData.get('type') === 'project') {
        revalidatePath(`/portfolio/${formData.get('categoryId')}`)
        redirect(`/portfolio/${formData.get('categoryId')}`, RedirectType.replace)
    }
    else if (formData.get('type') === 'blog') {
        revalidatePath('/blog')
        redirect('/blog', RedirectType.replace)
    }
}