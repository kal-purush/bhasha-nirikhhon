import { SituacaoUpdateInputObjectSchema } from "@/prisma/validation/schemas";
import { PrismaClient } from "@prisma/client";
import { NextRequest, NextResponse } from "next/server";

const prisma = new PrismaClient();

type Params = { id: number };

export async function GET(req: NextRequest, { id }: Params) {
    const data = await prisma.situacao.findFirst({ where: { id } });
    return NextResponse.json(data);
}

export async function PUT(req: NextRequest, { id }: Params) {
    try {
        const body = await req.json();
        const data = await SituacaoUpdateInputObjectSchema.parseAsync(body);
        const situacao = await prisma.situacao.update({ data, where: { id } });
        return NextResponse.json(situacao);
    } catch (e) {
        return NextResponse.error();
    }
}

export async function DELETE(req: NextRequest, { id }: Params) {
    try {
        const situacao = await prisma.situacao.delete({ where: { id } });
        return NextResponse.json(situacao);
    } catch (e) {
        return NextResponse.error();
    }
}