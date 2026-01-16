import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import openai

# Ensure OpenAI API key is loaded from the environment
AIPROXY_TOKEN = os.environ.get("AIPROXY_TOKEN")
if not AIPROXY_TOKEN:
    raise EnvironmentError("AIPROXY_TOKEN environment variable is not set.")

openai.api_key = AIPROXY_TOKEN

# Helper functions
def load_dataset(file_path):
    """Loads the dataset from the provided CSV file."""
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        raise ValueError(f"Error loading file {file_path}: {e}")


def analyze_data(data):
    """Performs a generic analysis of the dataset."""
    summary = {
        "shape": data.shape,
        "columns": data.dtypes.to_dict(),
        "missing_values": data.isnull().sum().to_dict(),
        "summary_statistics": data.describe(include='all').to_dict(),
    }
    return summary


def visualize_data(data):
    """Creates visualizations from the dataset."""
    images = []

    # Correlation heatmap (for numerical columns)
    if data.select_dtypes(include='number').shape[1] > 1:
        plt.figure(figsize=(10, 8))
        sns.heatmap(data.corr(), annot=True, fmt=".2f", cmap="coolwarm")
        plt.title("Correlation Heatmap")
        heatmap_path = "correlation_heatmap.png"
        plt.savefig(heatmap_path)
        plt.close()
        images.append(heatmap_path)

    # Distribution of numerical columns
    num_columns = data.select_dtypes(include='number').columns
    for col in num_columns:
        plt.figure(figsize=(8, 6))
        sns.histplot(data[col].dropna(), kde=True, bins=30)
        plt.title(f"Distribution of {col}")
        dist_path = f"distribution_{col}.png"
        plt.savefig(dist_path)
        plt.close()
        images.append(dist_path)

    return images


def generate_story(summary, visualizations):
    """Uses LLM to generate a story based on the analysis and visualizations."""
    prompt = (
        "Write a detailed narrative based on the following dataset analysis:\n"
        f"Summary: {summary}\n"
        f"Visualizations: {visualizations}\n"
        "Focus on describing the data, key insights, and actionable implications in Markdown format. "
        "Incorporate image references where relevant."
    )

    try:
        response = openai.Completion.create(
            engine="gpt-4o-mini",
            prompt=prompt,
            max_tokens=1500,
            temperature=0.7
        )
        return response["choices"][0]["text"].strip()
    except Exception as e:
        raise RuntimeError(f"Failed to generate story: {e}")


def save_readme(story):
    """Saves the generated story to README.md."""
    with open("README.md", "w") as f:
        f.write(story)


def main():
    if len(sys.argv) != 2:
        print("Usage: uv run autolysis.py <dataset.csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    # Load dataset
    print("Loading dataset...")
    data = load_dataset(file_path)

    # Analyze dataset
    print("Analyzing dataset...")
    summary = analyze_data(data)

    # Visualize dataset
    print("Creating visualizations...")
    visualizations = visualize_data(data)

    # Generate narrative
    print("Generating story...")
    story = generate_story(summary, visualizations)

    # Save narrative to README.md
    print("Saving results to README.md...")
    save_readme(story)

    print("Analysis complete. Files generated:")
    print("- README.md")
    for img in visualizations:
        print(f"- {img}")

if __name__ == "__main__":
    main()