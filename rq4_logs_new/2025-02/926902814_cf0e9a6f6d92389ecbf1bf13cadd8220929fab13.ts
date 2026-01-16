import { json, type RequestEvent } from '@sveltejs/kit';

export async function GET() {
  return json({ error: "Method not allowed" }, { status: 405 });
}

export async function POST({ request }: RequestEvent) {
  try {
    const { image } = await request.json();

    if (!image) {
      throw new Error("missing image");
    }

    const response = await fetch(`https://vision.googleapis.com/v1/images:annotate?key=${import.meta.env.VITE_CLOUD_VISION_API_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        requests: [
          { image: { content: image }, features: [{ type: 'TEXT_DETECTION' }] }
        ]
      })
    })

    const result = await response.json();
    if (!response.ok) {
      return json({ error: result.error.message }, { status: response.status });
    }

    const textAnnotations = result.responses?.[0]?.textAnnotations || [];
    const extractedText = textAnnotations.length > 0 ? textAnnotations[0].description : 'No text detected';

    return json({ text: extractedText });
  } catch (error) {
    if (error instanceof Error) {
      return json({ error: error.message }, { status: 400 });
    } else {
      return json({ error: "unkown error" }, { status: 400 });
    }
  }
}