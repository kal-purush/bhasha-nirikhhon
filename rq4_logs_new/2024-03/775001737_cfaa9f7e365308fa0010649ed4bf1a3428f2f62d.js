export async function GET() {
  const htmlResponse = await fetch("https://dev.dragonswap.app");
  const htmlBody = await htmlResponse.text();
  return new Response(htmlBody, {
    headers: {
      "content-type": "text/html",
    },
  });
}