import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from griptape.drivers import OpenAiChatPromptDriver
from griptape.loaders import ImageLoader
from griptape.rules import Rule
from griptape.structures import Workflow
from griptape.tasks import PromptTask

# Add the current working directory to the Python path
sys.path.insert(0, os.getcwd())


import gradio as gr

MAX_IMAGES = 150

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", None)


def run_captioning_with_griptape(
    images, concept_sentence, description_rules, openai_api_key, *captions
):
    print("run_captioning")
    print(f"concept sentence {concept_sentence}")
    print(f"captions {captions}")
    print(f"description rules {description_rules}")

    rules_list = description_rules.split("\n")
    rules = [Rule(rule) for rule in rules_list]
    captions = list(captions)  # Convert tuple to a list
    workflow = Workflow()
    driver = OpenAiChatPromptDriver(model="gpt-4o", api_key=openai_api_key)

    for i, image_path in enumerate(images):
        print(f"Processing image: {image_path}")

        # Load the input image artifact
        image_artifact = ImageLoader().load(Path(image_path).read_bytes())

        # Add an Describe the image to the workflow
        task = PromptTask(
            input=["Describe the image", image_artifact],
            prompt_driver=driver,
            rules=rules,
        )

        workflow.add_task(task)

    workflow.run()
    output_tasks = workflow.output_tasks

    # convert the result to a list
    for index, task in enumerate(output_tasks):
        captions[int(index)] = task.output.value
        yield captions


def add_griptape_options():
    # add a group to provide the user with the option to add rules to the descriptions
    with gr.Accordion("Caption Settings", open=True):
        description_rules = gr.Textbox(
            label="Rules for your captions",
            info="Add rules for image descriptions, separated by new lines.",
            placeholder="Examples:\nAlways mention the background color\nDescribe objects from left to right",
        )

        openai_api_key = gr.Textbox(
            label="OpenAI API Key",
            info="Add your OpenAI API key to use the Griptape captioning feature.",
            placeholder="sk-proj-...",
            value=OPENAI_API_KEY,
            type="password",
        )

    return description_rules, openai_api_key