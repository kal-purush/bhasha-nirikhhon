import IPost from "../../interfaces/IPost";
import { IPostFrontMatter } from "../../interfaces/IPostFrontMatter";
import { capitalizeFirstChar } from "../../utils";

export function contentResultsPageMetaTagInputs(content: any) {
  const title: string = "Content";
  const description: string =
    "Browse all blog posts in RIV.SYSTEMS, from professional work to journal entries and miscellaneous content.";

  return { description, title };
}

export function categoryResultsPageMetaTagInputs(
  content: any,
  slug: string | string[]
) {
  const category: string = slug[1];
  const title: string = capitalizeFirstChar(category);
  const description: string = `View all content related to ${title} in RIV.SYSTEMS.`;

  return { description, title };
}

export function sectionResultsPageMetaTagInputs(
  content: any,
  slug: string | string[]
) {
  const section: string = slug[0];
  const title: string = capitalizeFirstChar(section);
  const description: string = `View all content related to ${title} in RIV.SYSTEMS.`;

  return { description, title };
}

export function postPageMetaTagInputs(content: any) {
  const { frontmatter }: any = content;
  const {
    excerpt: description,
    coverImage: image,
    title,
  }: IPostFrontMatter = frontmatter;

  return { description, image, title };
}

export function getMetaTagInputs(
  content: IPost[][] | string[] | string | undefined,
  slug: string | string[] | undefined
) {
  switch (slug?.length) {
    case 1:
      return sectionResultsPageMetaTagInputs(content, slug);
    case 2:
      return categoryResultsPageMetaTagInputs(content, slug);
    case 3:
      return postPageMetaTagInputs(content);
    default:
      return contentResultsPageMetaTagInputs(content);
  }
}