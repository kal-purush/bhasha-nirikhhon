import type { GetStaticPaths, GetStaticProps } from 'next'
import { reorder } from '../util/reorder'
import type { BlogMeta } from '../util/types'
import type { WithSlug } from './getDirMeta.server'
import { getDirMeta } from './getDirMeta.server'
import { getDirPaths, removeExtension } from './getDirPaths.server'
import type { WithImageProps } from './getImageProps.server'
import { getImageProps } from './getImageProps.server'
import { getMDXData } from './getMDXData.server'

export type BlogMetaWithExtras = BlogMeta & WithSlug & WithImageProps

const BLOG_DIR = 'blog'

export const getBlogsMeta = async (): Promise<BlogMetaWithExtras[]> => {
    const allMeta = await getDirMeta<BlogMeta & WithSlug>(
        BLOG_DIR,
        fileName => '/blog/' + removeExtension(fileName)
    )
    const promises = allMeta.map(async m => {
        return { ...m, imageProps: await getImageProps(m.feature.href) }
    })
    const allMetaWithImageProps = await Promise.all(promises)
    return reorder(allMetaWithImageProps, 'date', true)
}

export const getStaticProps: GetStaticProps = async context => {
    const blogId = context.params?.id
    if (typeof blogId !== 'string') throw new Error('Blog id is not valid')
    const { meta, code } = await getMDXData<BlogMeta>(BLOG_DIR, blogId)
    const imageProps = await getImageProps(meta.feature.href)
    return { props: { meta: { ...meta, imageProps }, code } }
}

export const getStaticPaths: GetStaticPaths = async () => {
    const { fileNames } = await getDirPaths(BLOG_DIR)
    return {
        paths: fileNames.map(file => ({
            params: { id: removeExtension(file) },
        })),
        // Pregenerate all posts
        fallback: false,
    }
}