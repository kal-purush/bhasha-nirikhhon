import path from 'path'
import fs from 'fs/promises'
import matter from 'gray-matter'
import { getDirPaths, removeExtension } from './getDirPaths.server'

export interface WithSlug {
    slug: string
}

export async function getDirMeta<Meta extends WithSlug>(
    dir: string,
    createSlug: (fileName: string) => string
): Promise<Meta[]> {
    const { fileNames, directoryPath } = await getDirPaths(dir)

    const filePromises = fileNames.map((file: string) =>
        fs.readFile(path.join(directoryPath, file))
    )
    const files = await Promise.all(filePromises)

    const allMeta = files.reduce((arr, file, i) => {
        const meta = matter(file).data
        if (meta) {
            meta.slug = createSlug(fileNames[i])
            arr.push(meta as Meta)
        }
        return arr
    }, [] as Meta[])

    console.log(allMeta)
    return allMeta
}