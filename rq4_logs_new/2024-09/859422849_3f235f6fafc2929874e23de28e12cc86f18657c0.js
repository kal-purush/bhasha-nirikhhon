import { NextResponse } from "next/server";
import db from "@/lib/db";

export async function GET(request, { params: { slug } }) {
    try {
        const category = await db.category.findUnique({
            where: {
                slug:slug
            },
            include:{
                books:true
            }
        });
        if (!category) {
            return NextResponse.json({
                message: "Category not found"
            }, { status: 404 });
        }
        return NextResponse.json(category);
    } catch (error) {
        return NextResponse.json(
            {
                message: "Failed to fetch category",
                error
            }, { status: 500 }
        );
    }
}

