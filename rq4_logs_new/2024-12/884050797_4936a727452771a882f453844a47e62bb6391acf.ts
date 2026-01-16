import path, { join } from "node:path";
import fs from "node:fs";
import matter from "gray-matter";
import type Category from "../types/category";
import type Locale from "../types/locale";

const postDir = (locale: Locale = "ja") => {
    return join(process.cwd(), "src/_posts/", locale);
};

// export const getAllPosts = async () => {
//     const pathList = fs.readdirSync(postDir);
//     const contentsPromise = pathList.map(async (p) => {
//         const fullPath = path.join(postDir, p);
//         const fileContents = fs.readFileSync(fullPath, "utf8");
//         const { data, content } = matter(fileContents);
//         const slug = p.split(/\.mdx/)[0];

//         return {
//             data,
//             slug,
//             content,
//         };
//     });
//     const contents = await Promise.all(contentsPromise);

//     return contents;
// };

type dateOrder = "desc" | "asc";

export const getFilteredPosts = async (
    dateOrder: dateOrder = "desc",
    locale: Locale = "ja",
    category?: Category,
    tag?: string,
    country?: string,
) => {
    const pathList = fs.readdirSync(postDir(locale));
    const contentsPromise = pathList.map(async (p) => {
        const fullPath = path.join(postDir(locale), p);
        const filePath = fs.readFileSync(fullPath, "utf8");
        const { data, content } = matter(filePath);
        const slug = p.split(/\.mdx/)[0];

        return {
            data,
            slug,
            content,
        };
    });

    const contents = await Promise.all(contentsPromise);

    const filteredContents = contents.filter(({ data }) => {
        const matchesTag = tag ? data.tags?.includes(tag) : true;
        const matchesCategory = category ? data.category === category : true;
        const matchesCountry = country ? data["country?"] === country : true;
        return matchesTag && matchesCategory && matchesCountry;
    });

    const sortedContents = filteredContents.sort((a, b) => {
        const dateA = new Date(a.data.date);
        const dateB = new Date(b.data.date);

        return dateOrder === "asc"
            ? dateA.getTime() - dateB.getTime()
            : dateB.getTime() - dateA.getTime();
    });

    return sortedContents;
};

export const getPostBySlug = async (slug: string, locale: Locale = "ja") => {
    const fullPath = path.join(postDir(locale), `${slug}.mdx`);
    const fileContents = fs.readFileSync(fullPath, "utf8");
    const { data, content } = matter(fileContents);

    return {
        data,
        content,
    };
};

// export const getPostsByPage = (page: number, pageSize = 10) => {
//     const posts = getPosts();
//     const startIndex = (page - 1) * pageSize;
//     const endIndex = page * pageSize;

//     return posts.slice(startIndex, endIndex);
// };

// export const getTotalPages = (pageSize = 10) => {
//     const posts = getPosts();
//     const maxPage = Math.ceil(posts.length / pageSize);

//     return maxPage;
// };

// export const getTags = () => {
//     const posts = getPosts();
//     const tags = posts.flatMap((post) => post.frontmatter.tags || []);
//     const uniqueTags = [...new Set(tags)];

//     return uniqueTags;
// };

// export const getPostByEntryName = (entryName: string) => {
//     const posts = getPosts();
//     const post = posts.find((post) => post.entryName === entryName);
//     return post;
// };

// export const getLatestPostsWithoutTargetPost = (postEntryName: string) => {
//     const posts = getPosts();
//     const latestPosts = posts.filter(
//         (post) => post.entryName !== postEntryName,
//     );
//     return latestPosts.slice(0, 3);
// };

// export const getPostsFilteredByTag = (tag: string) => {
//     const allPosts = getPosts();
//     return allPosts.filter((post) => post.frontmatter.tags.includes(tag));
// };

// export const getTotalPagesFilteredByTag = (pageSize = 10, tag: string) => {
//     const posts = getPostsFilteredByTag(tag);
//     const maxPage = Math.ceil(posts.length / pageSize);

//     return maxPage;
// };