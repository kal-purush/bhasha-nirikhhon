import { PagesEnv } from "../post";

export const onRequest: PagesFunction<PagesEnv> = async ({
  request,
  params,
  env,
}) => {
  try {


    const inquiry = await env.INQUIRIES.get(params.name as string);
    if(!inquiry) throw new Error("Inquiry not found");
    const inquiryParsed = JSON.parse(inquiry);

    return new Response(JSON.stringify(inquiryParsed), {
      headers: {
        "content-type": "application/json",
      }
    });


  } catch (e) {
    if (e instanceof Error) {
      return new Response(e.message);
    }

    return new Response("Internal server error.", { status: 500 });
  }
};