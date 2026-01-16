import { DonorPaymentIProps, ParamsIdIProps } from "@/types";
import { NextResponse } from "next/server";

export const dynamic = 'force-dynamic'

export const GET = async (request: Request, { params }: ParamsIdIProps) => {
	try {
		const { id } = params;
		const result = await prisma?.donorPayment.findUnique({ where: { id } });
		return NextResponse.json(result);
	} catch (error) {
		throw new Error("Server Error")
	};
};

export const DELETE = async (request: Request, { params }: ParamsIdIProps) => {
	try {
		const { id } = params;
		await prisma?.donorPayment.delete({ where: { id } });
		return NextResponse.json({ message: "donor payment deleted" })
	} catch (error) {
		throw new Error("Server Error")
	}
};

export const PATCH = async (request: Request, { params }: ParamsIdIProps) => {
	try {
		const { id } = params;
		const body: DonorPaymentIProps = await request.json();
		const { amount, loanPayment } = body;
		const result = await prisma?.donorPayment.update({
			where: { id }, data: {
				amount, loanPayment
			}
		});
		return NextResponse.json(result);
	} catch (error) {
		throw new Error("Server Error")
	};
};