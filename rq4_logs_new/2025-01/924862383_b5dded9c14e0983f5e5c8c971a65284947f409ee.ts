import { NextRequest, NextResponse } from 'next/server'
import { createHash } from 'crypto'
import { gzip } from 'zlib'
import { promisify } from 'util'
import { createClient } from '@/utils/supabase/client'

const gzipAsync = promisify(gzip)
const supabase = createClient()

export async function GET(
  request: NextRequest,
  { params }: { params: { path: string[] } }
) {
  console.log('API route hit') // Debug log
  console.log('Full request URL:', request.url) // Log the full URL
  console.log('Request method:', request.method) // Log the HTTP method
  console.log('Params:', params) // Log the params

  const pathStr = params.path.join('/')
  console.log('Joined pathStr:', pathStr) // Log the processed path string

  try {
    if (pathStr === 'stable/Release') {
      console.log('Matched stable/Release') // Debug log
      return new NextResponse("Release", {
        status: 200,
        headers: { 'Content-Type': 'text/plain' }
      })
    } else if (pathStr === 'stable/Release.gpg') {
      console.log('Matched stable/Release.gpg') // Debug log
      return new NextResponse("Release.gpg", {
        status: 200,
        headers: { 'Content-Type': 'application/pgp-signature' }
      })
    } else if (pathStr === 'stable/main/binary-amd64/Packages.gz') {
      console.log('Matched Packages.gz') // Debug log
      return new NextResponse("Packages.gz", {
        status: 200,
        headers: { 'Content-Type': 'application/gzip' }
      })
    } else {
      console.log('No handler for path:', pathStr) // Debug logging
      return new NextResponse(null, { status: 404 })
    }
  } catch (error) {
    console.error('Error handling request:', error)
    return new NextResponse(JSON.stringify({ error: 'Internal server error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    })
  }
} 