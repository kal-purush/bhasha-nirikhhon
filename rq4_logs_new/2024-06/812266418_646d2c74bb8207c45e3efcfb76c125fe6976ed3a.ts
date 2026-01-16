'use server'

interface Project {
  title: string
  description: string
  type: 'BACKEND' | 'FRONTEND'
  imageUrl: string
  githubUrl: string
  appUrl?: string
}

export async function getPinnedProjects(): Promise<Project[]> {
  try {
    const res = await fetch(`${process.env.API_URL}/projects/pinned`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN}`,
      },
      next: {
        tags: ['getPinnedProjects'],
      },
    })

    const { projects }: { projects: Project[] } = await res.json()

    return projects
  } catch (error) {
    return []
  }
}

export async function getProjectsByTags(): Promise<{
  frontend: Project[]
  backend: Project[]
}> {
  try {
    const res = await fetch(`${process.env.API_URL}/projects`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${process.env.ACCESS_TOKEN}`,
      },
      next: {
        tags: ['getAllProjects'],
      },
    })

    const { projects }: { projects: Project[] } = await res.json()

    const frontend = projects.filter(({ type }) => type === 'FRONTEND')
    const backend = projects.filter(({ type }) => type === 'BACKEND')

    return { backend, frontend }
  } catch (error) {
    return { backend: [], frontend: [] }
  }
}