import { PagesEnv } from "../post";

export const onRequest: PagesFunction<PagesEnv> = async ({
  request,
  params,
  env,
}) => {
  try {
    // Delete if exists in R2
    await env.R2_INQUIRIES.delete(params.name as string);

    return new Response("true", {status:200});
  } catch (e) {
    if (e instanceof Error) {
      return new Response(e.message);
    }

    return new Response("Internal server error.", { status: 500 });
  }
};