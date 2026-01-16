import { NextResponse } from 'next/server'

export async function GET() {
  // This is a mock API. In a real-world scenario, you'd fetch this data from a database or external API
  const projects = [
    { id: 1, title: 'AI-Powered Chess Bot', description: 'A chess bot that uses machine learning to improve its gameplay.', image: '/placeholder.svg?height=200&width=300' },
    { id: 2, title: 'Smart Campus Navigation', description: 'An AR app for easy navigation around the university campus.', image: '/placeholder.svg?height=200&width=300' },
    { id: 3, title: 'IoT Weather Station', description: 'A network of IoT devices collecting real-time weather data across campus.', image: '/placeholder.svg?height=200&width=300' },
    { id: 4, title: 'Blockchain Voting System', description: 'A secure and transparent voting system using blockchain technology.', image: '/placeholder.svg?height=200&width=300' },
  ]

  return NextResponse.json(projects)
}
