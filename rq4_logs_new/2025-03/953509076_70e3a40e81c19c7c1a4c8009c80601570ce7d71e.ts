import { NextRequest, NextResponse } from 'next/server';
import categoriesData from '@/mocks/categories.json';
import { CategoryType } from '@/types';

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const tab = searchParams.get('tab') as CategoryType | null;

  if (tab === 'CONSULT') {
    return NextResponse.json(categoriesData.CONSULT);
  } else if (tab === 'USAGE') {
    return NextResponse.json(categoriesData.USAGE);
  }

  return NextResponse.json({
    CONSULT: categoriesData.CONSULT,
    USAGE: categoriesData.USAGE,
  });
}