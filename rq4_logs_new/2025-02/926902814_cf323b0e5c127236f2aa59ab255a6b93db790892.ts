import OpenAI from 'openai';

class OpenAIService {
	private static instance: OpenAI;

	private constructor() {}

	public static getInstance(): OpenAI {
		if (!this.instance) {
			this.instance = new OpenAI({
				apiKey: import.meta.env.VITE_OPENAI_API_KEY,
				// Optional: Add custom configuration
				dangerouslyAllowBrowser: false // Important: Prevent exposing API key client-side
			});
		}
		return this.instance;
	}
}

// Utility function for chat completion with error handling and streaming
export async function getChatCompletion(
	messages: OpenAI.ChatCompletionMessageParam[],
	options: {
		model: string;
		maxTokens: number;
		temperature: number;
	} = { model: 'gpt-4o-mini', maxTokens: 150, temperature: 0.7 }
) {
	const openai = OpenAIService.getInstance();

	try {
		const response = await openai.chat.completions.create({
			model: options.model,
			messages: messages,
			max_tokens: options.maxTokens,
			temperature: options.temperature,
			stream: false // Set to true for streaming responses
		});

		return response.choices[0].message.content;
	} catch (error) {
		console.error('OpenAI API Error:', error);
		throw new Error('Failed to get completion from OpenAI');
	}
}