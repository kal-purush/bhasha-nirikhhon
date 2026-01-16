import MarkdownIt from "markdown-it";
import markdownItLinkAttributes from "markdown-it-link-attributes";
import markdownItPrism from "markdown-it-prism";

export const renderMarkdown = (
    content: string,
    code: boolean = false,
    images: boolean = false,
): string => {
    let md = new MarkdownIt({}).use(markdownItLinkAttributes, {
        matcher: (href: string) => !href.startsWith("https://hkamran.com"),
        attrs: {
            target: "_blank",
            rel: "noopener noreferrer",
        },
    });
    if (code) {
        md = md.use(markdownItPrism, { plugins: ["toolbar", "autoloader"] });
    }

    let markdown = md.render(content);
    if (images) {
        markdown = markdown.replace(
            /<img([^>]+)>/gim,
            `<img$1 class="rounded-lg" />`,
        );
    }

    return markdown;
};