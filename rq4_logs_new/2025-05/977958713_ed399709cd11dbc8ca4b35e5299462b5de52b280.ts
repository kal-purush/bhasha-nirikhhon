import { NextResponse } from 'next/server';
import { readFile } from 'fs/promises';
import path from 'path';

export async function GET() {
  const configPath = path.join(process.cwd(), 'public/platform-config.json');
  const json = await readFile(configPath, 'utf-8');
  return NextResponse.json(JSON.parse(json));
}