// app/api/songs/[type]/[param]/route.ts
import { NextRequest, NextResponse } from 'next/server';

import {
  Brand,
  Period,
  getComposer,
  getLyricist,
  getNo,
  getPopular,
  getRelease,
  getSinger,
  getSong,
} from '@repo/api';

export async function GET(
  request: NextRequest,
  { params }: { params: { type: string; param: string } },
) {
  try {
    const { type, param } = params;
    const searchParams = request.nextUrl.searchParams;
    const brand = searchParams.get('brand') as Brand | undefined;

    let result = null;

    switch (type) {
      case 'title':
        result = await getSong({ title: param, brand });
        break;

      case 'singer':
        result = await getSinger({ singer: param, brand });
        break;

      case 'composer':
        result = await getComposer({ composer: param, brand });
        break;

      case 'lyricist':
        result = await getLyricist({ lyricist: param, brand });
        break;

      case 'no':
        result = await getNo({ no: param, brand });
        break;

      case 'release':
        result = await getRelease({ release: param, brand });
        break;

      case 'popular':
        // popular의 경우는 좀 특별하게 처리
        // param은 brand 값이 되고, period는 쿼리 파라미터로 받음
        const period = searchParams.get('period') as Period;
        if (!period) {
          return NextResponse.json(
            { error: '기간(period)은 필수 파라미터입니다.' },
            { status: 400 },
          );
        }
        result = await getPopular({ brand: param as Brand, period });
        break;

      default:
        return NextResponse.json({ error: '지원하지 않는 검색 유형입니다' }, { status: 400 });
    }

    if (!result) {
      return NextResponse.json({ error: '검색 결과가 없습니다' }, { status: 404 });
    }

    return NextResponse.json({ data: result });
  } catch (error) {
    console.error('API 요청 오류:', error);
    return NextResponse.json({ error: '서버 오류가 발생했습니다' }, { status: 500 });
  }
}