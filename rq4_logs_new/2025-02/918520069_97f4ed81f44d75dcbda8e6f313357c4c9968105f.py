import time
import os
from vllm import LLM, SamplingParams
from vllm.assets.audio import AudioAsset
os.environ["VLLM_TORCH_PROFILER_DIR"] = "./vllm_profile"

model = input('model? (tiny, small, medium, ...) > ')

# Create a Whisper encoder/decoder model instance
llm = LLM(
    model=f"openai/whisper-{model}",
    max_model_len=448,
    max_num_seqs=400,
    limit_mm_per_prompt={"audio": 1},
    kv_cache_dtype="fp8",
)

prompts = [
    {
        "prompt": "<|startoftranscript|>",
        "multi_modal_data": {
            "audio": AudioAsset("mary_had_lamb").audio_and_sample_rate,
        },
    },
    {  # Test explicit encoder/decoder prompt
        "encoder_prompt": {
            "prompt": "",
            "multi_modal_data": {
                "audio": AudioAsset("winning_call").audio_and_sample_rate,
            },
        },
        "decoder_prompt": "<|startoftranscript|>",
    }
] 

sampling_params = SamplingParams(
    temperature=0,
    top_p=1.0,
    max_tokens=200,
)

llm.start_profile()
outputs = llm.generate(prompts, sampling_params)
llm.stop_profile()


# Print the outputs.
for output in outputs:
    prompt = output.prompt
    encoder_prompt = output.encoder_prompt
    generated_text = output.outputs[0].text
    print(f"\nEncoder prompt: {encoder_prompt!r}, "
          f"\nDecoder prompt: {prompt!r}, "
          f"\nGenerated text: {generated_text!r}")

