import { setCookie } from "cookies-next";
import { AuthenticationClient } from "./AuthenticationClient";
import { AUTH_CONFIG } from "./Constants";
import validateCookie from "./validateCookie";

export default async function getNotebooks(
  context: any,
  userSelector?: string
) {
  const client = AuthenticationClient.init({
    ...AUTH_CONFIG,
    resource: `onenote/notebooks`,
  });
  const request = await client.api({ context, userSelector });
  const response = await request.executeRequest({
    shouldReturnProps: true,
  });

  const notebooks: any = [];
  if (response.props.value) {
    response.props.value.map((notebook: any) => {
      notebooks.push({
        id: notebook.id,
        displayName: notebook.displayName,
      });
    });
  }

  setCookie("notebooks", JSON.stringify(notebooks), {
    req: context.req,
    res: context.res,
    sameSite: "lax",
    path: "/",
  });

  return response;
}