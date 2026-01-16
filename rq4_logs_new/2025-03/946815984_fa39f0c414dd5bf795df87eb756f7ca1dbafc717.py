import openai
import os
import time
import docx
#dsdsds
# Folders
DOCUMENT_TO_ANALYZE_PATH = "document_to_analyze"
PROPRIETARY_FOLDER = "proprietary_documents"
RESULTS_FOLDER = "results"

# Ensure results folder exists
os.makedirs(RESULTS_FOLDER, exist_ok=True)

def read_docx(file_path):
    """Reads text from a .docx file."""
    doc = docx.Document(file_path)
    return "\n".join([para.text for para in doc.paragraphs])

def read_file(file_path):
    """Reads text from a .txt file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

def save_to_docx(text, output_path):
    """Saves text into a .docx file."""
    doc = docx.Document()
    doc.add_paragraph(text)
    doc.save(output_path)

def chatgpt_compare(prompt):
    """Sends a request to ChatGPT and retrieves response."""
    start_time = time.time()
    
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an expert document analyst."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.5
    )

    elapsed_time = time.time() - start_time
    tokens_used = response.usage.total_tokens
    cost = tokens_used / 1000 * 0.03  # Assuming GPT-4 costs $0.03 per 1K tokens
    result_text = response.choices[0].message.content

    return result_text, tokens_used, cost, elapsed_time

def main():
    # Find the first .docx file inside the document_to_analyze folder
    document_files = [f for f in os.listdir(DOCUMENT_TO_ANALYZE_PATH) if f.endswith(".docx")]

    if not document_files:
        print("Error: No .docx files found in 'document_to_analyze' folder.")
        return

    # Read the first document found inside the folder
    document_file_path = os.path.join(DOCUMENT_TO_ANALYZE_PATH, document_files[0])
    document_text = read_docx(document_file_path)

    # Save document text to .docx in results folder
    analyzed_docx_path = os.path.join(RESULTS_FOLDER, "analyzed_document.docx")
    save_to_docx(document_text, analyzed_docx_path)
    print(f"Document saved to {analyzed_docx_path}")

    # Read proprietary documents (.docx files)
    proprietary_texts = []
    proprietary_full_text = ""
    for file in os.listdir(PROPRIETARY_FOLDER):
        file_path = os.path.join(PROPRIETARY_FOLDER, file)
        if file.endswith(".docx"):  # Only process .docx files
            content = read_docx(file_path)
            proprietary_texts.append((file, content))
            proprietary_full_text += f"\n--- {file} ---\n{content}\n"
            #print(proprietary_full_text)
            
    # Save proprietary documents' input to a .docx file in results folder
    proprietary_docx_path = os.path.join(RESULTS_FOLDER, "proprietary_documents_input.docx")
    save_to_docx(proprietary_full_text, proprietary_docx_path)
    print(f"Proprietary documents input saved to {proprietary_docx_path}")

    # Prepare the prompt for ChatGPT
    comparison_prompt = f"""
        You are an expert **Compliance and Validation Analyst** specializing in regulatory compliance, risk assessment, and test plan validation.

        ### **Document to Analyze:**
        \"\"\"{document_text}\"\"\"

        ### **Proprietary Documents (Reference Materials)**
        """
    for filename, content in proprietary_texts:
        comparison_prompt += f"\n--- {filename} ---\n{content}\n"

    comparison_prompt += """
        You are an expert **Compliance and Validation Analyst** specializing in regulatory compliance, risk assessment, and test plan validation. 
        Your task is to analyze the document in the document_to_analyze folder against the documents in the proprietary_documents.
        Forget any previous conversation, prompts, or analysis. Treat this as a new, independent request with no prior context. 

        Analyze the document_to_analyze, which contains a list of system changes. For each change, determine the impacted requirements by referencing 
        the Trace Matrix table in the proprietary_documents folder. When a requirement is chosen, also tell me the associated Test Script number is located below the requirement.

        Once the impacted requirements are identified, retrieve their associated risk levels from the Risk Levels document in the same folder.

        Based on the risk level of each requirement, determine the necessary testing:

            High risk → Requires positive and negative testing
            Medium risk → Requires positive testing only
            Low risk → No testing required

        Return a structured output with:

            Each system change
            The impacted requirements with its description
            Their risk levels
            The required testing actions
            The Test Script number
            Certainty Score: [0-100] (indicating your confidence in the analysis)

            Ensure **every requirement has a certainty score** based on textual alignment, keyword relevance, and contextual meaning.
        """

    for filename, content in proprietary_texts:
        comparison_prompt += f"\n--- {filename} ---\n{content}\n"

    # Get response from ChatGPT
    result, tokens_used, cost, elapsed_time = chatgpt_compare(comparison_prompt)

    # Print results
    print("\n--- ChatGPT Analysis Result ---\n")
    print(result)
    print("\n--- API Usage ---")
    print(f"Tokens Used: {tokens_used}")
    print(f"Cost: ${cost:.4f}")
    print(f"Execution Time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()