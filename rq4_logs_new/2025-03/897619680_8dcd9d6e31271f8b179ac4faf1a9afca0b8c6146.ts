export type Post = PostAbstract & PostDetail;

type PostAbstract = {
    url: string;
    category: string;
    slug: string;
};

type PostDetail = PostMatter & {
    content: string;
    readingMinutes: number;
};

export type PostMatter = {
    title: string;
    date: string;
    author?: string;
    tags?: string[];
    excerpt?: string;
};

export const categoryList = [
    "All",
    "Study",
    "Next.js Blog",
    "CS",
    "Algorithm",
    "React",
    "JavaScript",
    "TypeScript",
    "Python",
    "Java",
    "Spring",
    "Chart js",
    "BootStrap",
    "Tailwind CSS",
    "WebSocket",
];