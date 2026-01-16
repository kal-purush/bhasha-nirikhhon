import prisma from '@/lib/db'
import { NextResponse } from "next/server";

export async function POST(req) {
	const { category } = await req.json()

	const newCategory = await prisma.category.create({ data: { ...category } })

	return NextResponse.json(newCategory)
}