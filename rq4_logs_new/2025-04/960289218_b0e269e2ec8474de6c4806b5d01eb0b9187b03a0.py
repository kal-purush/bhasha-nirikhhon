from jinja2 import Environment, FileSystemLoader
from .model.Project import Risk, Budget, Objective, ProjectDescription, ProjectDescriptionSection, BudgetSection, ObjectiveSection, RiskSection
import markdown
from markdown.extensions.extra import ExtraExtension
from fastapi import FastAPI, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional


# Example project
# See model/Project.py for the model definitions
###############
example_project = ProjectDescription(
    project=ProjectDescriptionSection(
        project_name="Ny togbane ml. Samsø og Læsø",
        content="Dette er et eksempel på en projektbeskrivelse."
    ),
    start_date="2023-01-01",
    end_date="2023-12-31",
    stakeholders=["Jens Jensen", "Peter Petersen"],
    budget=BudgetSection(
        budget=[
            Budget(name="Læsø Togstation", amount=100000),
            Budget(name="Samsø Togstation", amount=110000),
            Budget(name="Skinner", amount=1000000),
        ],
        content="Dette er en oversigt over projektets budget."
    ),
    objectives=ObjectiveSection(
        objectives=[
            Objective(name="Opført station på Samsø", start_date="2023-01-01", end_date="2023-06-30"),
            Objective(name="Opført station på Læsø", start_date="2023-01-01", end_date="2023-06-30"),
            Objective(name="Udbyg skinner mellem Læsø og Samsø", start_date="2023-06-30", end_date="2024-01-01"),
        ],
        content="Dette er en oversigt over projektets mål og tidslinje."
    ),
    risks=RiskSection(
        risks=[
            Risk(name="Forsinkelse af Samsø station", impact=5, likelihood=3),
            Risk(name="Forsinkelse af Læsø station", impact=4, likelihood=4),
            Risk(name="Forsinkelse af skinner", impact=3, likelihood=2),
        ],
        content="Dette er en oversigt over projektets risici."
    )
)
###############

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_project_template():
    """
    Loads the skabelon.md file from the data/skabelon directory.
    Returns:
        str: The content of the skabelon.md file.
    """
    file_loader = FileSystemLoader("data/skabelon")
    env = Environment(loader=file_loader)
    template = env.get_template("skabelon.md")

    return template


def load_result_template():
    """
    Loads the result.html file from the app/html directory.
    Returns:
        str: The content of the result.html file.
    """
    file_loader = FileSystemLoader("app/html")
    env = Environment(loader=file_loader)
    template = env.get_template("result.html")

    return template

def load_index():
    """
    Loads the index.html file from the app/html directory.
    Returns:
        str: The content of the index.html file.
    """
    with open("app/html/index.html", "r") as file:
        html = file.read()
    return html

def load_instructions():
    """
    Loads the instructions for each chapter from the template file.
    Returns:
        dict: A dictionary of instructions where the key is the chapter name and the value is the instruction text.
    """
    template = load_project_template()
    return template.module.instructions

def render_project_description(project: ProjectDescription) -> str:
    """
    Renders the project description as HTML using the provided Pydantic model.

    Args:
        project (ProjectDescription): The project description model.
    Returns:
        str: The rendered HTML content.
    """
    html_template = load_result_template()
    project_template = load_project_template()
    
    # Render the markdown template with the project data
    markdown_text = project_template.render(**project.model_dump())
    html_text = markdown.markdown(
        markdown_text, extensions=["extra"], output_format="html5"
    )

    # Replace Mermaid code blocks with divs for rendering
    html_text = html_text.replace(
        '<code class="language-mermaid">', '<div class="mermaid">'
    ).replace("</code>", "</div>")

    return html_template.render(html_content=html_text)


def render_markdown_to_html(markdown_text: str) -> str:
    """
    Renders markdown text to HTML. Use this function to render any markdown content.

    Args:
        markdown_text (str): The markdown text to render.
    Returns:
        str: The rendered HTML content.
    """
    html_template = load_result_template()
    html_text = markdown.markdown(
        markdown_text, extensions=["extra"], output_format="html5"
    )
    return html_template.render(html_content=html_text)


@app.get("/", response_class=HTMLResponse)
async def html_landing():
    return load_index()


@app.post("/generate", response_class=HTMLResponse)
async def generate_project(
    files: Optional[List[UploadFile]] = File(None)
):
    """
    Handles form submission to generate a new project description.
    Args:
        headings (List[str]): Selected headings from the form.
        files (List[UploadFile]): Uploaded files.
    Returns:
        HTMLResponse: Rendered project description.
    """
    print(files) # You need to implement the logic to handle the uploaded files and headings
    # Example: Render the example project description
    return HTMLResponse(content=render_project_description(example_project))


# if __name__ == "__main__":
#     import uvicorn

#     uvicorn.run(app, host="0.0.0.0", port=8000)