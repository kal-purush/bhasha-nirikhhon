from openai import AsyncAzureOpenAI
from openai.helpers import LocalAudioPlayer
from dotenv import load_dotenv
import os
import asyncio

load_dotenv()

client = AsyncAzureOpenAI(
    api_key = os.environ["AZURE_OPENAI_API_KEY"],  
    api_version = os.environ["AZURE_OPENAI_API_VERSION"],
    azure_endpoint = os.environ["AZURE_OPENAI_API_ENDPOINT"]
    )

input = """We’re excited to launch the new gpt-4o-mini-tts model on Azure OpenAI. Developers can now “instruct” the model not just on what to say but how to say it—enabling more customised experiences for use cases ranging from customer service to creative storytelling.\n\nWe're also thrilled to share that the new gpt-4o-transcribe and gpt-4o-mini-transcribe models are now available to deploy in Azure OpenAI. These new speech to text models have improved word error rates and better language recognition and accuracy, compared to the original Whisper models.\n\nDeploy them in Azure AI Foundry, and try them today!"""

instructions = """Voice: Confident, resonant, and vibrant—commanding attention and generating excitement.\n\nTone: Energetic, optimistic, and authoritative, creating anticipation and highlighting the significance of announcements.\n\nDelivery: Clear and dynamic, with strategic pauses to emphasise key moments and build suspense before major reveals.\n\nEmotion: Enthusiastic, celebratory, and inclusive, making attendees feel they are part of a ground-breaking moment.\n\nPunctuation: Crisp, impactful phrases, using exclamation points and well-timed pauses to heighten engagement and dramatic effect.\n\nPronunciation: Precise articulation of technical terms with enthusiastic emphasis to underline their importance.\n\nPersonality Affect: Professional yet approachable, blending credibility with genuine excitement to energise and unify the audience."""

async def main() -> None:

    async with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="ballad",
        input=input,
        instructions=instructions,
        response_format="pcm",
    ) as response:
        await LocalAudioPlayer().play(response)

if __name__ == "__main__":
    asyncio.run(main())