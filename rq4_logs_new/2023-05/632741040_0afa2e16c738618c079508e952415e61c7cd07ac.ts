import { customAlphabet } from 'nanoid'

export const slugify = (text: string, maxWords: number = 10): string => {
  const slug = text
    .toString()
    .toLowerCase()
    .replace(/\s+/g, '-') // Replace spaces with hyphens
    .replace(/[^a-z0-9-]/g, '') // Remove non-alphanumeric characters except hyphens
    .replace(/-{2,}/g, '-') // Replace multiple consecutive hyphens with a single hyphen
    .replace(/(^-+|-+$)/g, '') // Trim hyphens from the beginning and end
    .split('-')
    .slice(0, maxWords)
    .join('-')
  const nanoid = customAlphabet('1234567890abcdef', 10)

  return `${slug}-${nanoid()}`
}