import { fetchJupiterPrice } from "@/lib/jupiter";
import { NextRequest } from "next/server";

export async function POST(request:NextRequest) {
    const data = await fetchJupiterPrice('JUP')
    const JupPrice = data.data.JUP.price;
    console.log(JupPrice)
}