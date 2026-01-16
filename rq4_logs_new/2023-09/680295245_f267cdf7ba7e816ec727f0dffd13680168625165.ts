// ../providers/replicate.ts
import Replicate from 'replicate';
import { AnthropicStream, StreamingTextResponse } from 'ai';

import { cartridges } from "@/lib/data"
import { Cartridge } from "@/lib/Cartridge";
import { InputOpts } from '@/lib/InputOpts';
import getSystemPromptStub from '../helpers/systemPrompts'
import build from 'next/dist/build';

export const runtime = 'edge';

export default async function anthropicProvider(
    cartridgeId: number,
    input: string,
    inputLang: string,
    outputLang: string,
    //inputOpts should be InputOpts or null
    inputOpts: InputOpts | null
) {

    const replicate = new Replicate({
        auth: process.env.REPLICATE_API_KEY || '',
    });

    const cartridge = cartridges.find((c: Cartridge) => c.id === cartridgeId);
    if (!cartridge) {
        //the whole system is not great if this happens
        throw new Error('Cartridge not found');
    } else {
        const providerOpts = cartridge.providerOpts;

        if (inputOpts === null) {
            // Handle error
            throw new Error('Sorry, something went wrong with inputs');
        } else {
            const translateFlavor = inputOpts.translateFlavor;
            const translateExplain = inputOpts.translateExplain;

            // Note we are not yet constructing systemPrompt with more detail.
            // This will eventually be a function of the cartridge and the user's settings.
            // e.g., const systemPrompt = await getSystemPrompt(cartridge, translateFlavor, isExplain);
            const systemPrompt = getSystemPromptStub(inputLang, outputLang, translateFlavor, translateExplain);
            const buildPrompt = (input: string, systemPrompt: string) => {
                // format prompts like this for anthropic:
                //    \n\nHuman: {input}\n\nAssistant:{systemPrompt}\n\n

                return `\n\nHuman: ${input}\n\nAssistant:${systemPrompt}\n\n`;
            }

            // Ask Anthropic for an API response
            const response = await fetch('https://api.anthropic.com/v1/complete', { //nice if this were parameterized
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': process.env.ANTHROPIC_API_KEY || '', //TODO SET THIS
                },
                body: JSON.stringify({
                    prompt: buildPrompt(input, systemPrompt),
                    model: cartridge.providerOpts.modelId,
                    max_tokens_to_sample: cartridge.providerOpts.maxTokens,
                    temperature: cartridge.providerOpts.temperature,
                    stream: cartridge.providerOpts.streaming,
                }),
            });

            // Check for errors
            if (!response.ok) {
                return new Response(await response.text(), {
                    status: response.status,
                });
            }
            const stream = AnthropicStream(response);
            return stream;
        }
    }
}