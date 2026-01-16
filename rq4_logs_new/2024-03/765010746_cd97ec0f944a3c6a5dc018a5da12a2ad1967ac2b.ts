import { supabase } from '@/utils/supabaseClient'
import type { NextApiResponse } from 'next'
import { notFound } from 'next/navigation';
import { NextResponse } from 'next/server'


export async function GET(req: Request, res: NextApiResponse) {
    const id = req.url.split("/blog/")[1];
    const { data, error } = await supabase
        .from('posts')
        .select('*').eq("id", id).single();
    if (error) {
        return NextResponse.error();
    }
    if (!data) {
        notFound()
    }
    return NextResponse.json(data, { status: 200 })
}

export const PUT = async (req: Request, res: NextApiResponse) => {
    const id = req.url.split("/blog/")[1];
    const { title, content } = await req.json();
    const { data, error } = await supabase
        .from('posts').update({ title, content, createdAt: new Date().toISOString() }).eq("id", id);
    if (error) {
        return NextResponse.error();
    }
    return NextResponse.json(data, { status: 200 })
}

export const DELETE = async (req: Request, res: NextApiResponse) => {
    const id = req.url.split("/blog/")[1];
    const { data, error } = await supabase
        .from('posts').delete().eq("id", id);
    if (error) {
        return NextResponse.error();
    }
    return NextResponse.json(data, { status: 200 })
}