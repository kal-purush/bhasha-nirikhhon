import {NextResponse} from "next/server";
import {auth} from "@/auth";
import {Roll, jsonToRoll} from "@/types/roll";
import redis from "@/lib/redis";
import {checkUserAuthenticated, checkUserRole} from "@/lib/auth";

async function addRoll(userId: string, roll: Roll): Promise<void> {
    const key = `user:${userId}:rolls`;
    const res = await redis.lpush(key, JSON.stringify(roll));
    console.log(`Redis set ${key} result: ${res}`);
}

async function getRolls(userId: string, cursor = 0, pageSize = 20): Promise<Roll[]> {
    const key = `user:${userId}:rolls`;
    const rolls = await redis.lrange(key, cursor, pageSize-1);
    return rolls.map((roll) => JSON.parse(roll));
}

export async function POST(request: Request): Promise<NextResponse> {
    const session = await auth();

    const unauthenticatedResponse = checkUserAuthenticated(session);
    if (unauthenticatedResponse) {
        return unauthenticatedResponse;
    }

    const unauthorizedResponse = checkUserRole(session, ["admin", "player"]);
    if (unauthorizedResponse) {
        return unauthorizedResponse;
    }

    if (!request.body) {
        return new NextResponse(null, {status: 400});
    }

    const json = await request.json()
    if (!json) {
        return new NextResponse(null, {status: 400});
    }

    let roll: Roll;
    try {
        roll = jsonToRoll(json);
    } catch (e) {
        console.error('Error parsing roll', e);
        return NextResponse.json(
            {error: "Error parsing roll"},
            {status: 400});
    }

    try {
        await addRoll(session!.user!.id, roll);
        console.log('Roll added', roll);
    } catch (error) {
        console.error('Error adding roll', error);
        return NextResponse.json(
            {error: 'Error adding roll'},
            {status: 500}
        );
    }

    return NextResponse.json(
        {message: 'Roll added successfully'},
        {status: 201}
    );
}


export async function GET(request: Request) {
    const session = await auth();

    const unauthenticatedResponse = checkUserAuthenticated(session);
    if (unauthenticatedResponse) {
        return unauthenticatedResponse;
    }

    const unauthorizedResponse = checkUserRole(session, ["admin", "player"]);
    if (unauthorizedResponse) {
        return unauthorizedResponse;
    }

    const {searchParams} = new URL(request.url);
    const cursor = parseInt(searchParams.get('cursor') || '0', 10);
    const pageSize = parseInt(searchParams.get('page_size') || '20', 10);

    try {
        const rolls = await getRolls(session!.user!.id, cursor, pageSize);
        return NextResponse.json(rolls);
    } catch (error) {
        console.error('Error getting rolls', error);
        return NextResponse.json(
            {error: 'Error getting rolls'},
            {status: 500}
        );
    }
}