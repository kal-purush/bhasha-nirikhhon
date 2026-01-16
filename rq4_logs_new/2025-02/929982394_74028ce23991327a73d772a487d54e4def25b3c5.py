import spacy
from spacy.matcher import PhraseMatcher

def load_job_description(file_path):
    """Load a job description from a text file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return ""

def extract_skills_spacy(text, skills_list):
    """
    Extract skills from text using spaCy's PhraseMatcher.
    
    Args:
        text (str): The job description text.
        skills_list (list of str): List of skills to match.
    
    Returns:
        set: A set of matched skills.
    """
    nlp = spacy.load("en_core_web_sm")
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    
    # Create patterns for each skill
    patterns = [nlp.make_doc(skill) for skill in skills_list]
    matcher.add("SKILL", patterns)
    
    doc = nlp(text)
    matches = matcher(doc)
    
    found_skills = set()
    for match_id, start, end in matches:
        span = doc[start:end]
        found_skills.add(span.text)
    
    return found_skills

if __name__ == '__main__':
    # Path to your job description file.
    job_description_file = 'job_description.txt'
    job_description = load_job_description(job_description_file)
    
    # Define a list of relevant skills.
    skills_list = [
        "Python", "Java", "SQL", "machine learning", "data analysis", "communication",
        "agile", "cloud", "AWS", "Docker", "Kubernetes", "JavaScript", "React", "Node.js",
        "data warehousing", "ETL", "statistics", "R", "C++"
    ]
    
    skills_found = extract_skills_spacy(job_description, skills_list)
    
    print("Skills extracted using spaCy:")
    for skill in skills_found:
        print(f"- {skill}")