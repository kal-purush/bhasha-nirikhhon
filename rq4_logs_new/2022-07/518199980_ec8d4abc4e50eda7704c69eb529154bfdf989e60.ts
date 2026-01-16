import { setCookie } from "cookies-next";
import { AuthenticationClient } from "./AuthenticationClient";
import { AUTH_CONFIG } from "./Constants";
import validateCookie from "./validateCookie";

export default async function getTemplates(context: any) {
  const parsedSection = validateCookie({ cookie: context, key: "section" });
  const sectionId = parsedSection.id;

  const client = AuthenticationClient.init({
    ...AUTH_CONFIG,
    resource: `onenote/sections/${sectionId}/pages`,
  });
  const request = await client.api({ context });
  const response = await request.executeRequest({
    shouldReturnProps: true,
  });

  const templates: any = [];
  if (response.props.value) {
    response.props.value.map((page: any) => {
      templates.push({
        id: page.id,
        title: page.title,
        contentUrl: page.contentUrl,
      });
    });
  }

  setCookie("templates", JSON.stringify(templates), {
    req: context.req,
    res: context.res,
    sameSite: "lax",
    path: "/",
  });

  return response;
}