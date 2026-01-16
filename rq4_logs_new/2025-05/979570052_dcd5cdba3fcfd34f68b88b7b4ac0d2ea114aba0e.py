"""Config class to load and validate environment variables."""

import logging
import os

from dotenv import load_dotenv


class Config:
    """Configuration class to load and validate environment variables."""

    def __init__(self) -> None:
        load_dotenv()
        self.bot_token = os.getenv("BOT_TOKEN")
        self.channel_id = os.getenv("CHANNEL_ID")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_base_url = os.getenv("OPENAI_BASE_URL")
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.gemini_image_model = os.getenv("GEMINI_IMAGE_MODEL")
        self.image_prompt_start = os.getenv("IMAGE_PROMPT_START")
        self.dry_run = eval(os.getenv("DRY_RUN", "False"))
        self.openai_model = os.getenv("OPENAI_MODEL")
        self.max_context_chars = int(os.getenv("MAX_CONTEXT_CHARS", "15000"))
        self.initial_story_idea = os.getenv("INITIAL_STORY_IDEA")
        self.story_max_sentences = int(os.getenv("STORY_MAX_SENTENCES", "500"))
        self.poll_question_template = "Как продолжится история?"
        self.fallback_continue_prompt = "Продолжай как считаешь нужным."
        self.end_story_option = "Закончить историю"

    def validate(self) -> bool:
        """Validate the configuration loaded from environment variables."""
        valid = True
        if not self.bot_token:
            logging.error("BOT_TOKEN is not set correctly.")
            valid = False
        if not self.channel_id:
            logging.error("CHANNEL_ID is not set correctly.")
            valid = False
        if not self.initial_story_idea:
            logging.error("INITIAL_STORY_IDEA cannot be empty.")
            valid = False
        if not self.openai_api_key or not self.openai_base_url:
            logging.error("Missing OpenAI API key or base URL.")
            valid = False
        if (
            not self.gemini_api_key
            or not self.gemini_image_model
            or not self.image_prompt_start
        ):
            logging.error(
                "GEMINI_API_KEY and GEMINI_IMAGE_MODEL "
                "and IMAGE_PROMPT_START cannot be empty.",
            )
            valid = False
        if not self.initial_story_idea:
            logging.error("INITIAL_STORY_IDEA cannot be empty.")
            valid = False
        return valid