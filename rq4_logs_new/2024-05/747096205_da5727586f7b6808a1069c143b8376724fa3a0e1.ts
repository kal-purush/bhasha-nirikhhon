import prisma from '@/lib/prisma'
import { IForm } from '@/types/forms'
import { prismaConnect } from '@/utils/prismaConnect'
import { NextResponse } from 'next/server'

export async function PATCH(request: Request, { params }: { params: { id: string } }) {
  try {
    if (!params.id) {
      return new NextResponse('Form Not found', { status: 404 })
    }
    await prismaConnect()
    const data: IForm = await request.json()
    const updatedForm = await prisma.form.update({
      where: {
        id: params.id
      },
      data: {
        name: data.name,
        formLink: data.formLink,
        excelLink: data.excelLink
      }
    })
    return NextResponse.json(updatedForm, { status: 200 })
  } catch (error) {
    console.log('[UPDATE FORM]', error)
    return new NextResponse('Internal Server Error', { status: 500 })
  }
}

export async function DELETE(request: Request, { params }: { params: { id: string } }) {
  if (!params.id) {
    return new NextResponse('Not found', { status: 404 })
  }
  try {
    await prismaConnect()
    await prisma.form.delete({
      where: {
        id: params.id
      }
    })
    return NextResponse.json({ message: 'form deleted successfully' }, { status: 200 })
  } catch (error) {
    console.log('[DELETE FORM', error)
    return NextResponse.json({ message: 'Cannot fetch' }, { status: 500 })
  }
}