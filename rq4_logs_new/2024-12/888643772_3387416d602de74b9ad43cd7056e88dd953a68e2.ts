import { mockResponse } from "@/constants/mock";
const API_BASE_URL = process.env.EXPO_PUBLIC_ESSENCE_REST_API_URL;

export const getArticleFromServer = async (publicKey: string) => {
  try {
    const headers: { [key: string]: string } = {
      "Content-Type": "application/json",
    };

    const config: RequestInit = {
      method: "GET",
      headers,
      body: undefined,
    };

    const response = await fetch(
      `${API_BASE_URL}/public/article/${publicKey}`,
      config,
    );

    if (!response.ok) {
      throw new Error("Failed to get article from server");
    }
    const article = await response.json();
    return article;
  } catch (error) {
    console.error("Failed to get article from server:", error);
    throw new Error("Failed to get article from server");
  }
};

export async function GET(request: Request) {
  // return Response.json({ hello: 'world' });
  const url = new URL(request.url);
  const newsId = url.searchParams.get("newsId") || "";
  console.log({ newsId });
  const article = await getArticleFromServer(newsId);
  const response = mockResponse
    .replaceAll("ARTICLE_TITLE", article.title)
    .replaceAll("ARTICLE_IMAGE", article.image)
    .replaceAll("ARTICLE_DESCRIPTION", article.summary_50)
    .replaceAll("ARTICLE_URL", article.url)
    .replaceAll("PUBLIC_KEY_HERE", newsId);

  return new Response(response, {
    status: 200,
    headers: {
      "Content-Type": "text/html",
    },
  });
}