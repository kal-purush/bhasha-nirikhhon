import prisma from '@/lib/prisma'
import { prismaConnect } from '@/utils/prismaConnect'
import { NextResponse } from 'next/server'
import { TTestimonialResponse } from '@/types/testimonials'

export async function GET(request: Request, { params }: { params: { id: string } }) {
  if (!params.id) {
    return new NextResponse('Not found', { status: 404 })
  }
  try {
    await prismaConnect()
    const review = await prisma.testimonial.findUnique({
      where: {
        id: params.id
      }
    })
    if (!review) {
      return new NextResponse('Review Not found', { status: 404 })
    }
    return NextResponse.json(review, { status: 200 })
  } catch (error) {
    console.log('[GET TESTIMONIAL BY ID]', error)
    return NextResponse.json({ message: 'Cannot fetch' }, { status: 500 })
  }
}

export async function PATCH(request: Request, { params }: { params: { id: string } }) {
  try {
    if (!params.id) {
      return new NextResponse('Teacher Not found', { status: 404 })
    }
    await prismaConnect()
    const { name, email, message } = await request.json()
    const updatedTestimonial: TTestimonialResponse = await prisma.testimonial.update({
      where: {
        id: params.id
      },
      data: {
        name,
        email,
        message,
        isPublished: true
      }
    })
    return NextResponse.json(updatedTestimonial, { status: 200 })
  } catch (error) {
    console.log('[UPDATE TESTIMONIAL]', error)
    return new NextResponse('Internal Server Error', { status: 500 })
  }
}

export async function DELETE(request: Request, { params }: { params: { id: string } }) {
  if (!params.id) {
    return new NextResponse('Not found', { status: 404 })
  }
  try {
    await prismaConnect()
    await prisma.testimonial.delete({
      where: {
        id: params.id
      }
    })

    return NextResponse.json({ message: 'review deleted successfully' }, { status: 200 })
  } catch (error) {
    console.log('[DELETE TESTIMONIAL', error)
    return NextResponse.json({ message: 'Cannot fetch' }, { status: 500 })
  }
}