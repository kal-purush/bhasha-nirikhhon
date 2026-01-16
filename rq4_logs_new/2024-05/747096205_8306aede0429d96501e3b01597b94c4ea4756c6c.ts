import prisma from '@/lib/prisma'
import { prismaConnect } from '@/utils/prismaConnect'
import { NextResponse } from 'next/server'
import { IPartners } from '@/types/partners'

export async function GET(request: Request, { params }: { params: { id: string } }) {
  if (!params.id) {
    return new NextResponse('Not found', { status: 404 })
  }
  try {
    await prismaConnect()
    const partner = await prisma.partners.findUnique({
      where: {
        id: params.id
      }
    })
    if (!partner) {
      return new NextResponse('Partner Not found', { status: 404 })
    }
    return NextResponse.json(partner, { status: 200 })
  } catch (error) {
    console.log('[GET PARTNER BY ID]', error)
    return NextResponse.json({ message: 'Cannot fetch' }, { status: 500 })
  }
}

export async function PATCH(request: Request, { params }: { params: { id: string } }) {
    try {
      if (!params.id) {
        return new NextResponse('Patrner Not found', { status: 404 })
      }
      await prismaConnect()
      const data: IPartners = await request.json()
      const updatedPartner = await prisma.partners.update({
        where: {
          id: params.id
        },
        data: {
            nameUa: data.nameUa,
            nameEn: data.nameEn,
            imageUrl: data.imageUrl,
            imageId: data.imageId,
            descriptionUa: data.descriptionUa,
            descriptionEn: data.descriptionEn,
            color: data.color,
            websiteLink: data.websiteLink
        }
      })
      return NextResponse.json(updatedPartner, { status: 200 })
    } catch (error) {
      console.log('[UPDATE Partner]', error)
      return new NextResponse('Internal Server Error', { status: 500 })
    }
  }
  
  export async function DELETE(request: Request, { params }: { params: { id: string } }) {
    if (!params.id) {
      return new NextResponse('Partner Not found', { status: 404 })
    }
    try {
      await prismaConnect()
      await prisma.partners.delete({
        where: {
          id: params.id
        }
      })
  
      return NextResponse.json({ message: 'partner deleted successfully' }, { status: 200 })
    } catch (error) {
      console.log('[DELETE PARTNER', error)
      return NextResponse.json({ message: 'Cannot fetch' }, { status: 500 })
    }
  }