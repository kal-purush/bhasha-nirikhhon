import { PrismaClient } from "@prisma/client";
import { NextResponse } from "next/server";
import { main } from "../route";

const prisma = new PrismaClient();

// 詳細取得
export const GET = async (req: Request, res: NextResponse) => {
	try {
		const id: number = parseInt(req.url.split("/portfolio/")[1]);
		await main();
		const portfolio = await prisma.portfolio.findFirst({ where: { id } });
		return NextResponse.json(
			{ message: "Success", portfolio },
			{ status: 200 }
		);
	} catch (err) {
		return NextResponse.json({ message: "Error", err }, { status: 500 });
	} finally {
		await prisma.$disconnect();
	}
};

// 編集
export const PUT = async (req: Request, res: NextResponse) => {
	try {
		const id: number = parseInt(req.url.split("/portfolio/")[1]);
		const { title, description, overview } = await req.json();
		await main();
		const portfolio = await prisma.portfolio.update({
			data: { title, description, overview },
			where: { id },
		});
		return NextResponse.json(
			{ message: "Success", portfolio },
			{ status: 200 }
		);
	} catch (err) {
		return NextResponse.json({ message: "Error", err }, { status: 500 });
	} finally {
		await prisma.$disconnect();
	}
};

// 削除
export const DELETE = async (req: Request, res: NextResponse) => {
	try {
		const id: number = parseInt(req.url.split("/portfolio/")[1]);
		await main();
		const portfolio = await prisma.portfolio.delete({
			where: { id },
		});
		return NextResponse.json(
			{ message: "Success", portfolio },
			{ status: 200 }
		);
	} catch (err) {
		return NextResponse.json({ message: "Error", err }, { status: 500 });
	} finally {
		await prisma.$disconnect();
	}
};