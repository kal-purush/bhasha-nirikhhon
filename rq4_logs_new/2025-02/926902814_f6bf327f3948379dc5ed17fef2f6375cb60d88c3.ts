import { json } from '@sveltejs/kit';

export async function GET() {
    try {
        const response = await fetch('https://api.openai.com/v1/models', {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${import.meta.env.VITE_OPENAI_API_KEY}`,
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`Error fetching models: ${response.statusText}`);
        }

        const data = await response.json();
        return json(data);
    } catch (error: unknown) {
        return json({ error: error}, { status: 500 });
    }
}