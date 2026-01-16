import { NextResponse } from "next/server";
import { NextRequest } from "next/server";
const baseUrl = "http://localhost:3000";

export default function middleware(req: NextRequest){
    let verify = req.cookies.get("token");
    let url = req.url
    
    if(!verify && (url.includes('/journey') || url.includes('/profile') || url.includes('/profile/edit'))){
        return NextResponse.redirect(baseUrl+"/auth/signin");
    }

    if (verify && (url.includes('/auth/signin') || url.includes('/auth/signup'))) {
      return NextResponse.redirect(baseUrl + "/journey");
    }
}