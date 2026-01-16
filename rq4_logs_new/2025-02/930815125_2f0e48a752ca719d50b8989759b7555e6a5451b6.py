import time
import json
import streamlit as st
import g4f
from PyPDF2 import PdfReader
import plotly.express as px
import pandas as pd

#############################################
# Custom CSS for Modern, Attractive UI
#############################################
def local_css():
    st.markdown(
        """
        <style>
        /* Set the overall background */
        .stApp {
            background: #f0f2f6;
        }
        /* Sidebar background */
        .css-1d391kg, .css-1lcbmhc {  
            background-color: #ffffff;
        }
        /* Header styles */
        h1 {
            color: #007BFF;
            font-family: 'Segoe UI', sans-serif;
        }
        h2, h3 {
            color: #333333;
            font-family: 'Segoe UI', sans-serif;
        }
        /* Button styling */
        div.stButton > button {
            background-color: #007BFF;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 5px;
            font-size: 16px;
            font-weight: 500;
            box-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        div.stButton > button:hover {
            background-color: #0056b3;
        }
        /* Text area styling */
        .stTextArea textarea {
            border: 2px solid #007BFF;
            border-radius: 4px;
        }
        /* Radio button styling */
        .stRadio > label {
            font-size: 16px;
        }
        /* Form container styling */
        .css-1siy2j7 {
            background: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

#############################################
# Utility Functions
#############################################
def extract_text_from_pdf(file):
    """Extract text from a PDF file."""
    pdf_reader = PdfReader(file)
    text = ""
    for page in pdf_reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text
    return text.strip()

def generate_response(prompt):
    """Generate a response using GPT-4 (via g4f)."""
    try:
        response = g4f.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            top_p=0.9
        )
        return response.strip() if response else "Chatbot: Sorry, I didn't understand that."
    except Exception as e:
        return f"Chatbot: Error: {e}"

def simulate_typing(text, delay=0.005):
    """Simulate a typing effect in the Streamlit UI."""
    message_placeholder = st.empty()
    full_response = ""
    for char in text:
        full_response += char
        time.sleep(delay)
        message_placeholder.markdown(full_response + "▌")
    message_placeholder.markdown(full_response)

#############################################
# Core Functionalities
#############################################
def analyze_resume(resume_text):
    """Analyze resume text using GPT-4."""
    prompt = f"""
    Analyze the following resume text and provide comprehensive insights on the candidate's skills, experience, education, and potential career opportunities:
    
    Resume Text:
    {resume_text}
    
    Please provide a detailed analysis covering:
    1. Key Technical Skills
    2. Professional Experience Highlights
    3. Educational Background
    4. Potential Career Growth Areas
    5. Recommended Skill Development Paths
    """
    return generate_response(prompt)

def generate_mcq_for_skills(resume_text):
    """Generate multiple-choice questions (MCQs) for key skills extracted from the resume."""
    prompt = f"""
    Based on the following resume text, identify the candidate's key technical skills. For each key skill, generate 3 multiple-choice questions (MCQs) that test the candidate's knowledge about that skill. Each question must have one correct answer and three plausible incorrect options.

    **IMPORTANT:** Output ONLY valid JSON (no explanations, no markdown) in the exact format below.

    Example format:
    [
      {{
        "skill": "Python",
        "questions": [
          {{
             "question": "What does the 'len' function do in Python?",
             "options": {{
                 "a": "Returns the number of items in an object",
                 "b": "Calculates the length of a string in bytes",
                 "c": "Deletes an object",
                 "d": "None of the above"
             }},
             "correct": "a"
          }},
          ... (2 more questions)
        ]
      }},
      ... (other skills)
    ]

    Resume Text:
    {resume_text}
    """
    return generate_response(prompt)

def parse_mcq_json(mcq_json_text):
    """Parse the JSON output of the MCQs."""
    try:
        mcq_data = json.loads(mcq_json_text)
        return mcq_data
    except Exception as e:
        st.error(f"JSON parsing error: {e}")
        st.text_area("Raw MCQ JSON", mcq_json_text, height=300)
        return None

def suggest_learning_platforms(resume_text, mcq_data, score, total_questions):
    """Generate personalized learning recommendations based on quiz performance."""
    prompt = f"""
    Based on the following resume and MCQ test results, provide comprehensive learning platform recommendations:

    Resume Text:
    {resume_text}

    MCQ Test Performance:
    - Total Questions: {total_questions}
    - Score: {score}
    - Performance Percentage: {(score/total_questions)*100:.2f}%

    Skills Tested:
    {json.dumps(mcq_data, indent=2)}

    Please provide:
    1. A detailed analysis of the candidate's skill gaps
    2. Tailored learning platform recommendations for each skill
    3. Suggested learning paths to improve weak areas
    4. Platforms that offer certifications or advanced courses
    5. A motivational message encouraging continuous learning

    The recommendations should be comprehensive, actionable, and personalized. Consider both free and paid platforms, online courses, and certification programs.
    """
    try:
        recommendations = generate_response(prompt)
        return recommendations
    except Exception as e:
        return f"Error generating recommendations: {e}"

def generate_cover_letter(resume_text, job_description):
    """Generate a tailored cover letter based on resume and job description."""
    prompt = f"""
    Using the following resume and job description, generate a tailored cover letter for the candidate.
    
    Resume:
    {resume_text}
    
    Job Description:
    {job_description}
    
    Please include the candidate's key strengths, relevant experience, and motivation for the role.
    """
    return generate_response(prompt)

def analyze_job_description(resume_text, job_description):
    """Compare resume with a job description and suggest improvements."""
    prompt = f"""
    Compare the following resume and job description. Highlight how well the candidate's skills match the job requirements and provide suggestions on how to better align the resume with the job requirements.
    
    Resume:
    {resume_text}
    
    Job Description:
    {job_description}
    """
    return generate_response(prompt)

#############################################
# Main Application with Sidebar Navigation
#############################################
def main():
    st.set_page_config(page_title="Modern Resume Analyzer", page_icon="📊", layout="wide")
    
    # Inject custom CSS for modern UI
    local_css()
    
    # App header
    st.markdown("<h1 style='text-align: center;'>Modern Resume Analyzer & Learning Path</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: #555;'>Empower Your Career Journey with AI-driven Insights</h3>", unsafe_allow_html=True)
    
    # Sidebar Navigation and File Uploads
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.selectbox("Choose a Module", 
                                    ["About", "Resume Analysis", "Skills Quiz", "Learning Recommendations", "Cover Letter Generator", "Job Description Analyzer", "Download Report"])
    
    st.sidebar.subheader("Upload Files")
    uploaded_resume = st.sidebar.file_uploader("Upload your resume (PDF or TXT)", type=["pdf", "txt"], key="resume")
    if uploaded_resume:
        if "resume_text" not in st.session_state:
            if uploaded_resume.type == "application/pdf":
                st.session_state.resume_text = extract_text_from_pdf(uploaded_resume)
            elif uploaded_resume.type == "text/plain":
                st.session_state.resume_text = uploaded_resume.read().decode("utf-8")
    
    uploaded_job_desc = st.sidebar.file_uploader("Upload Job Description (optional)", type=["pdf", "txt"], key="jobdesc")
    job_desc_text = ""
    if uploaded_job_desc:
        if uploaded_job_desc.type == "application/pdf":
            job_desc_text = extract_text_from_pdf(uploaded_job_desc)
        elif uploaded_job_desc.type == "text/plain":
            job_desc_text = uploaded_job_desc.read().decode("utf-8")
    
    # ------------------------ About Page ------------------------
    if app_mode == "About":
        st.markdown("<h2>About This App</h2>", unsafe_allow_html=True)
        st.write("""
        This modern web application leverages GPT‑4 to help you analyze your resume, test your skills with interactive quizzes, 
        and generate personalized learning recommendations. It also includes a cover letter generator and job description analyzer 
        to further empower your job search process.
        
        **Features:**
        - **Resume Analysis:** Get detailed insights about your resume.
        - **Skills Quiz:** Evaluate your skills through a dynamically generated quiz.
        - **Learning Recommendations:** Receive tailored learning suggestions based on your performance.
        - **Cover Letter Generator:** Create customized cover letters for job applications.
        - **Job Description Analyzer:** Compare your resume with a job description to identify areas for improvement.
        - **Downloadable Report:** Compile all your insights and results into a downloadable report.
        """)
    
    # For modules requiring a resume upload, warn the user if not available.
    if app_mode != "About" and "resume_text" not in st.session_state:
        st.warning("Please upload your resume using the sidebar to continue.")
        return

    resume_text = st.session_state.get("resume_text", "")
    
    # ------------------------ Resume Analysis ------------------------
    if app_mode == "Resume Analysis":
        st.markdown("<h2>Resume Analysis</h2>", unsafe_allow_html=True)
        st.subheader("Extracted Resume Text")
        st.text_area("Resume Content", resume_text, height=300)
        if st.button("Analyze Resume"):
            with st.spinner("Analyzing your resume..."):
                analysis = analyze_resume(resume_text)
            st.subheader("Analysis Result")
            simulate_typing(analysis)
    
    # ------------------------ Skills Quiz ------------------------
    if app_mode == "Skills Quiz":
        st.markdown("<h2>Skills Assessment Quiz</h2>", unsafe_allow_html=True)
        st.info("Generating a quiz based on your resume skills...")
        with st.spinner("Generating skills quiz..."):
            mcq_json_text = generate_mcq_for_skills(resume_text)
        if st.checkbox("Show raw MCQ JSON output for debugging"):
            st.text_area("Raw MCQ JSON", mcq_json_text, height=300)
        
        mcq_data = parse_mcq_json(mcq_json_text)
        if mcq_data:
            st.write("Answer the following questions:")
            quiz_form = st.form("quiz_form")
            for skill_block in mcq_data:
                skill_name = skill_block.get("skill", "Unknown Skill")
                skill_key = skill_name.replace(" ", "_")
                quiz_form.markdown(f"<h3>Skill: {skill_name}</h3>", unsafe_allow_html=True)
                questions = skill_block.get("questions", [])
                for idx, q in enumerate(questions):
                    q_key = f"{skill_key}_{idx}"
                    quiz_form.markdown(f"<b>Question:</b> {q.get('question', 'No question provided')}", unsafe_allow_html=True)
                    options = q.get("options", {})
                    options_display = { key: f"{key}) {value}" for key, value in options.items() }
                    quiz_form.radio("Select an answer:", list(options_display.keys()),
                                    index=0, format_func=lambda x: options_display[x],
                                    key=q_key)
                    quiz_form.markdown("<hr>", unsafe_allow_html=True)
            submitted = quiz_form.form_submit_button("Submit Answers")
            if submitted:
                score = 0
                question_count = 0
                detailed_results = []
                skill_scores = {}
                st.markdown("<h3>Quiz Results</h3>", unsafe_allow_html=True)
                for skill_block in mcq_data:
                    skill_name = skill_block.get("skill", "Unknown Skill")
                    skill_key = skill_name.replace(" ", "_")
                    questions = skill_block.get("questions", [])
                    for idx, q in enumerate(questions):
                        question_count += 1
                        q_key = f"{skill_key}_{idx}"
                        user_answer = st.session_state.get(q_key)
                        correct_answer = q.get("correct", "").lower()
                        question_text = q.get("question", "No question provided")
                        result = {
                            "skill": skill_name,
                            "question": question_text,
                            "user_answer": user_answer,
                            "correct_answer": correct_answer
                        }
                        st.markdown(f"<b>Question:</b> {question_text}", unsafe_allow_html=True)
                        if user_answer == correct_answer:
                            score += 1
                            st.success(f"Your answer: {user_answer} (Correct)")
                            result["status"] = "Correct"
                            skill_scores.setdefault(skill_name, {"correct": 0, "total": 0})
                            skill_scores[skill_name]["correct"] += 1
                        else:
                            st.error(f"Your answer: {user_answer} (Incorrect). Correct answer: {correct_answer}")
                            result["status"] = "Incorrect"
                            skill_scores.setdefault(skill_name, {"correct": 0, "total": 0})
                        skill_scores[skill_name]["total"] = skill_scores[skill_name].get("total", 0) + 1
                        detailed_results.append(result)
                        st.markdown("<hr>", unsafe_allow_html=True)
                performance_percentage = (score / question_count) * 100
                st.info(f"Overall Score: {score} out of {question_count} ({performance_percentage:.2f}%)")
                if performance_percentage >= 80:
                    st.balloons()
                    performance_rating = "Excellent"
                elif performance_percentage >= 60:
                    performance_rating = "Good"
                elif performance_percentage >= 40:
                    performance_rating = "Average"
                else:
                    performance_rating = "Needs Improvement"
                st.markdown(f"<h4>Performance Rating: {performance_rating}</h4>", unsafe_allow_html=True)
                
                if skill_scores:
                    df = pd.DataFrame([
                        {"Skill": skill,
                         "Correct": data["correct"],
                         "Total": data["total"],
                         "Percentage": (data["correct"] / data["total"]) * 100}
                        for skill, data in skill_scores.items()
                    ])
                    fig = px.bar(df, x="Skill", y="Percentage",
                                 title="Skill-wise Performance (%)",
                                 text="Percentage",
                                 range_y=[0, 100],
                                 color="Skill",
                                 template="plotly_white")
                    st.plotly_chart(fig)
                st.session_state.quiz_results = {
                    "score": score,
                    "total": question_count,
                    "detailed_results": detailed_results,
                    "skill_scores": skill_scores
                }
    
    # ------------------------ Learning Recommendations ------------------------
    if app_mode == "Learning Recommendations":
        st.markdown("<h2>Learning Recommendations</h2>", unsafe_allow_html=True)
        if "quiz_results" not in st.session_state:
            st.warning("Please complete the Skills Quiz first.")
        else:
            quiz_results = st.session_state.quiz_results
            with st.spinner("Generating personalized learning recommendations..."):
                recommendations = suggest_learning_platforms(
                    resume_text,
                    mcq_data if 'mcq_data' in locals() else [],
                    quiz_results["score"],
                    quiz_results["total"]
                )
            simulate_typing(recommendations)
            st.session_state.recommendations = recommendations
    
    # ------------------------ Cover Letter Generator ------------------------
    if app_mode == "Cover Letter Generator":
        st.markdown("<h2>Cover Letter Generator</h2>", unsafe_allow_html=True)
        st.write("Provide a job description below to generate a tailored cover letter.")
        job_desc_input = st.text_area("Job Description", job_desc_text, height=200)
        if st.button("Generate Cover Letter"):
            if not job_desc_input.strip():
                st.error("Please provide a job description.")
            else:
                with st.spinner("Generating your cover letter..."):
                    cover_letter = generate_cover_letter(resume_text, job_desc_input)
                st.subheader("Your Cover Letter")
                simulate_typing(cover_letter)
                st.session_state.cover_letter = cover_letter
    
    # ------------------------ Job Description Analyzer ------------------------
    if app_mode == "Job Description Analyzer":
        st.markdown("<h2>Job Description Analyzer</h2>", unsafe_allow_html=True)
        st.write("Compare your resume with a job description to identify areas for improvement.")
        job_desc_input = st.text_area("Job Description", job_desc_text, height=200)
        if st.button("Analyze Job Description"):
            if not job_desc_input.strip():
                st.error("Please provide a job description.")
            else:
                with st.spinner("Analyzing job description..."):
                    analysis_result = analyze_job_description(resume_text, job_desc_input)
                st.subheader("Analysis Result")
                simulate_typing(analysis_result)
                st.session_state.jd_analysis = analysis_result
    
    # ------------------------ Download Report ------------------------
    if app_mode == "Download Report":
        st.markdown("<h2>Download Report</h2>", unsafe_allow_html=True)
        report_sections = []
        if "resume_text" in st.session_state:
            report_sections.append("----- RESUME TEXT -----\n" + resume_text)
        if "quiz_results" in st.session_state:
            quiz_info = st.session_state.quiz_results
            report_sections.append("----- QUIZ RESULTS -----\n" + f"Score: {quiz_info['score']} out of {quiz_info['total']}\nDetailed Results:\n{json.dumps(quiz_info['detailed_results'], indent=2)}")
        if "recommendations" in st.session_state:
            report_sections.append("----- LEARNING RECOMMENDATIONS -----\n" + st.session_state.recommendations)
        if "cover_letter" in st.session_state:
            report_sections.append("----- COVER LETTER -----\n" + st.session_state.cover_letter)
        if "jd_analysis" in st.session_state:
            report_sections.append("----- JOB DESCRIPTION ANALYSIS -----\n" + st.session_state.jd_analysis)
        
        report_content = "\n\n".join(report_sections)
        st.text_area("Report Preview", report_content, height=400)
        st.download_button("Download Report", data=report_content, file_name="resume_report.txt", mime="text/plain")

if __name__ == "__main__":
    main()