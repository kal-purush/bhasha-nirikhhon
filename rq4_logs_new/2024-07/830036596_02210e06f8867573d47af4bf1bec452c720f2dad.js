import { pipeline, env } from '@xenova/transformers';
env.allowLocalModels = false;
env.useBrowserCache = false;

/**
 * This class uses the Singleton pattern to ensure that only one instance of the
 * pipeline is loaded. This is because loading the pipeline is an expensive
 * operation and we don't want to do it every time we want to translate a sentence.
 */
class MyPipeline {
    static task = 'text-generation';
    static model = 'Xenova/distilgpt2';
    static instance = null;

    static async getInstance(progress_callback = null) {
        if (this.instance === null) {
            this.instance = pipeline(this.task, this.model, { progress_callback });
        }

        return this.instance;
    }
}

// Listen for messages from the main thread
self.addEventListener('message', async (event) => {
    // Retrieve the translation pipeline. When called for the first time,
    // this will load the pipeline and save it for future use.
    let recommender = await MyPipeline.getInstance(x => {
        // We also add a progress callback to the pipeline so that we can
        // track model loading.
        self.postMessage(x);
    });

    // Actually perform the translation
    let output = await recommender(event.data.text, {
        temperature: 0.2,
        max_new_tokens: 200,
        repetition_penalty: 1.5,
        no_repeat_ngram_size: 2,
        num_beams: 2,
        num_return_sequences: 2,
        // Allows for partial output
        callback_function: x => {
            self.postMessage({
                status: 'update',
                output: recommender.tokenizer.decode(x[0].output_token_ids, { skip_special_tokens: true })
            });
        }
    });

    // Send the output back to the main thread
    self.postMessage({
        status: 'complete',
        output: output,
    });
});