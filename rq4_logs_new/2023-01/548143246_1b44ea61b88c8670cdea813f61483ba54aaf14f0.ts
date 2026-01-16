import { getPlaiceholder } from "plaiceholder";
import IPost from "../../interfaces/IPost";

export async function includePlaceholderImage(
  content: IPost[][] | string[] | string | undefined
) {
  if (!content) return;

  const contentIsForResultsPage = Array.isArray(content);

  if (contentIsForResultsPage) {
    const contentWithPlaceholderImage = await Promise.all(
      content.map(async (singleContent: any) => {
        const { coverImage } = singleContent.frontmatter;
        const { base64 } = await getPlaiceholder(coverImage);

        singleContent.frontmatter.placeholderImage = base64;

        return singleContent;
      })
    );

    return contentWithPlaceholderImage;
  } else {
    // @ts-ignore
    const { coverImage } = content.frontmatter;
    const { base64 } = await getPlaiceholder(coverImage);

    // @ts-ignore
    content.frontmatter.placeholderImage = base64;

    return content;
  }
}