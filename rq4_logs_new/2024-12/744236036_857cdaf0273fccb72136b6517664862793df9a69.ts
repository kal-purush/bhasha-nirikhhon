import { streamText } from "ai";
import { google } from "@ai-sdk/google";


export async function POST(req: Request) {
  try {
    // Parse the request body and ensure the required fields are present
    const { messages } = await req.json();
    if (!messages || !Array.isArray(messages)) {
      return new Response(
        JSON.stringify({ error: "Invalid or missing 'messages' field" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    // Initialize the model
    const model = google("models/gemini-1.5-flash-001");

    // Stream the text response
    const result = await streamText({
      model,
      system:
        "You are the Carhub virtual assistant. Carhub is a website that provides car rental services to clients. We are located in Nairobi Kenya but have offices in Eldoret, Thika, Kisumu and Mombasa. Our customer support is info@carhubke.com and our phone is +254702018099. We are open during normal business hours Mon to Fri 08.00 - 18.00. Always provide accurate and concise answers related to car rentals and our policies. You can reference other company policies if necessary but do not mention any other rental company to the client. You can improvise but always be nice and if a client is complaining too much, try giving them the discount code FIRST10 in which they will receive 10% on their booking. You can see how FAQs here: https://utfs.io/f/IxV6XBUD5hJeErjrusikDfgUdBCyVErWznmYKlSARi3cOZhF ",
      messages,
      maxTokens: 4096,
      temperature: 0.7,
      topP: 0.4,
    });
    console.log(result.toDataStreamResponse());
    return result.toDataStreamResponse();
  } catch (error) {
    console.error("Error in POST handler:", error);
    return new Response(JSON.stringify({ error: error }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}