import { NextRequest, NextResponse } from "next/server";
import {contact_schema} from "@/app/validationSchema";
import { z } from "zod";

type ContactData = z.infer<typeof contact_schema>;

let DUMMY_CONTAINER: ContactData[] = [];

export async function POST (request: NextRequest){

    const body = await request.json();

    const data_validation = contact_schema.safeParse(body);

    if(!data_validation.success) return NextResponse.json( data_validation.error.format(), {status: 400});

    DUMMY_CONTAINER.push({
        name: body.name,
        email: body.email,
        phone: body.phone,
        message: body.message,
    });
    console.log(DUMMY_CONTAINER);
    
    return NextResponse.json(DUMMY_CONTAINER, {status: 200});
}