import * as vscode from "vscode";
import { APIProvider } from "./apiProvider";
import { Ollama } from "langchain/llms/ollama";


export interface OllamaParams {
    model: string;
    prompts:any[];
    // max_tokens: number;
    temperature: number;
    // stream: boolean;
}

export class ollamaLLMProvider extends APIProvider {
    private ollama: Ollama | undefined;
    private context: vscode.ExtensionContext;
    private baseUrl: string | undefined;
  
    constructor(context: vscode.ExtensionContext) {
        super();
        this.context = context;
        console.log(`ollama initialized!!`);
    }
  
    async init() {
        this.baseUrl = "http://localhost:11434"

    }
    
    async completeStream(params: OllamaParams , callbacks: any) {
        const initialRecentPrompt = params.prompts[params.prompts.length-1]["content"];
        const codellamaInitialRecentPrompt = "Give me only code block about subsequent requests with triple backticks. " + initialRecentPrompt;

        const customPrompts = params.model === "codellama" ? codellamaInitialRecentPrompt : initialRecentPrompt;
        this.ollama = new Ollama({
            baseUrl: this.baseUrl, // Default value
            model: params.model, 
            verbose:true,
            temperature: params.temperature
        });


        if (!this.baseUrl) {
            throw new Error("ollama API is not initialized.");
        }

        try {

            const stream = await this.ollama.stream(
                customPrompts
            );
                
                let buffer = "";
                let gptMessage = "";
                
                for await (const chunk of stream) {
                    
                    try {
                        if (chunk) {
                            gptMessage += chunk;
                            if (callbacks.onUpdate) {
                                callbacks.onUpdate(gptMessage);
                            }
                        }
                    } catch (error) {
                        console.error("Error parsing message:", error);
                        continue;
                    }
                }

                callbacks.onComplete(gptMessage);
                
                                    
        } catch (error: any) {
            console.error("Error fetching stream:", error);
            throw error;
        }
    }
}