import pandas as pd
import random

# Load data from CSV files
data_analytics = pd.read_csv('data_analytics_new.csv').to_dict(orient='records')
full_stack = pd.read_csv('full_stack_new.csv').to_dict(orient='records')
python = pd.read_csv('python_new.csv').to_dict(orient='records')

def generate_mcqs(data, question_type, num_questions=5, course_name=""):
    mcqs = []
    selected_indices = random.sample(range(len(data)), min(num_questions, len(data)))  # Select unique indices
    for index in selected_indices:
        item = data[index]
        concept = item["Concept"]
        description = item["Description"]
        
        # Generate options
        options = [description]
        while len(options) < 4:
            random_item = random.choice(data)
            if random_item["Description"] not in options:
                options.append(random_item["Description"])
        
        random.shuffle(options)  # Shuffle options
        
        # Create question based on type
        if question_type == "descriptive":
            question = f"What is {concept}?"
        elif question_type == "application":
            question = f"Which of the following best describes the use of {concept}?"
        elif question_type == "analytical":
            if len(data) > 1:
                other_concept = random.choice([item for item in data if item["Concept"] != concept])
                question = f"What is the main difference between {concept} and {other_concept['Concept']}?"
                options = [f"{concept} is for {description}", f"{other_concept['Concept']} is for {other_concept['Description']}"]
                options += [random.choice(data)["Description"] for _ in range(2)]
                random.shuffle(options)
        
        mcqs.append({
            "question": question,
            "options": options,
            "answer": description,
            "course": course_name  # Tagging the course
        })
    
    return mcqs

# Generate MCQs
data_analytics_mcqs = generate_mcqs(data_analytics, "descriptive", course_name="Data Analytics")
full_stack_mcqs = generate_mcqs(full_stack, "application", course_name="Full Stack")
python_mcqs = generate_mcqs(python, "analytical", course_name="Python")

# Combine all MCQs
all_mcqs = data_analytics_mcqs + full_stack_mcqs + python_mcqs

def conduct_assessment(mcqs):
    scores = {
        "Data Analytics": 0,
        "Full Stack": 0,
        "Python": 0
    }
    
    for mcq in mcqs:
        print(mcq["question"])
        for idx, option in enumerate(mcq["options"], start=1):
            print(f"{idx}. {option}")
        
        answer = input("Your answer (1-4): ")
        if mcq["options"][int(answer) - 1] == mcq["answer"]:
            scores[mcq["course"]] += 1  # Increment score for the corresponding course
        print()  # New line for better readability
    
    return scores

# Conduct the assessment
scores = conduct_assessment(all_mcqs)
print(f"Scores: {scores}")

def analyze_performance(scores):
    feedback = {}
    for course, score in scores.items():
        if score >= 4:  # Assuming full marks are 5
            feedback[course] = "You are strong in this area."
        elif score >= 2:
            feedback[course] = "You have a moderate understanding in this area."
        else:
            feedback[course] = "You need improvement in this area."
    return feedback

def recommend_course(scores):
    # Find the course with the highest score
    recommended_course = max(scores, key=scores.get)
    return recommended_course

# Analyze performance
performance_feedback = analyze_performance(scores)
for course, feedback in performance_feedback.items():
    print(f"{course}: {feedback}")

# Recommend a course based on the scores
recommended_course = recommend_course(scores)
print(f"\nBased on your performance, we suggest you consider a course in {recommended_course}.")
print("However, the final decision to take the course is yours.")