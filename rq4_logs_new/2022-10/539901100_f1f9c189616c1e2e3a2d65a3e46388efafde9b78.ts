import { PagesEnv } from "../post";

export const onRequest: PagesFunction<PagesEnv> = async ({
  request,
  env,
}) => {
  try {
    const inquiryFiles = await env.R2_INQUIRIES.list({limit: 100});

    let inquiryFilesMapped = inquiryFiles.objects.map(async (object) => {
      return {object};
    });

    return new Response(JSON.stringify(await Promise.all(inquiryFilesMapped)), {
      headers: {
        "content-type": "application/json",
      },
    });
  } catch (e) {
    if (e instanceof Error) {
      return new Response(e.message);
    }

    return new Response("Internal server error.", { status: 500 });
  }
};