import { NextResponse } from "next/server";
import jwt from "jsonwebtoken";
import User, { IUser } from "@/models/User";

const SECRET_KEY = process.env.AUTH_SECRET || 'default_secret_key';


export async function POST(req: Request){
    const {username, password} = await req.json();
    const user: IUser | null = await User.findOne({name:username});

    if(!user){
        return NextResponse.json({erroe: "Invalid credentials"}, {status: 401});
    }
    const token = jwt.sign({username: user.name, role: user.role}, SECRET_KEY,{
        expiresIn: "1h",
    });
    return NextResponse.json({token});
}