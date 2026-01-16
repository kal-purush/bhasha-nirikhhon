export const dynamic = 'force-static'
import { type NextRequest } from 'next/server'
import { getPrograms } from '@/lib/firebase/firestore';
import { connect, getBasePdf, uploadBasePdf } from '@/lib/mongo/client';

export async function GET(request: NextRequest, { params }: { params: Promise<{ program: string }> }) {
  let { program } = await params;

  const validity = await validateDetails(program);

  program = program.toLowerCase();

  if (!validity.success) {
    return new Response(validity.body, { status: validity.status });
  }
  await connect();
  const pdf = await getBasePdf(program);
  if (!pdf) return new Response('File not found', { status: 404 });
  return new Response(pdf, {
    headers: {
      'Content-Type': 'application/pdf',
    }
  });
}

export async function POST(request: NextRequest, { params }: { params: Promise<{ program: string }> }) {
  let { program } = await params;
  // get pdf file
  const formData = await request.formData();
  const body = Object.fromEntries(formData);
  const file = (body.file as Blob) || null;

  if (!file) return new Response('No file in request', { status: 400 });

  if (!file.type.match('application/pdf')) return new Response('Invalid file type', { status: 400 });

  const validity = await validateDetails(program);

  program = program.toLowerCase();

  if (!validity.success) {
    return new Response(validity.body, { status: validity.status });
  }

  await connect();
  const res = await uploadBasePdf(new File([file], `${program}.pdf`, {
    type: 'application/pdf',
  }), program);
  console.log("Upload response:", res);
  if (!res) return new Response('Error uploading file', { status: 500 });
  return new Response(JSON.stringify({
    success: true,
    url: `/admin/templates/${program}`,
  }), {
    status: 200,
  });
}

async function validateDetails(programSlug: string) {
  if (!programSlug.toLowerCase().match(/^[a-z0-9_]+$/)) return {
    success: false,
    status: 400,
    body: 'Invalid ID or Program',
  };

  const programs = await getPrograms();
  if (!programs.includes(programSlug.toLowerCase())) return {
    success: false,
    status: 404,
    body: 'Program not found',
  };

  return {
    success: true,
    status: 200,
    body: 'OK',
  };
}