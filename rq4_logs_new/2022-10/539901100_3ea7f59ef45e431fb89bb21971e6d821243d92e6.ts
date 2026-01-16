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

    // Delete if exists from KV
    if(inquiryParsed) await env.INQUIRIES.delete(params.name as string);
    // Delete if exists in R2
    if(inquiryParsed?.file) await env.R2_INQUIRIES.delete(`file-${inquiryParsed.date}`);

    return new Response("true", {status:200});
  } catch (e) {
    if (e instanceof Error) {
      return new Response(e.message);
    }

    return new Response("Internal server error.", { status: 500 });
  }
};