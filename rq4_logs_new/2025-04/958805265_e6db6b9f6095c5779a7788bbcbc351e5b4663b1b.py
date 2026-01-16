import os
import json
import openai
import time
import sys

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

# ----------------------------------------------------------------------------
# 1. Load configuration
# ----------------------------------------------------------------------------
# We'll assume you have a config.json with your OpenAI API key:
#
# {
#   "api_key": "sk-..."
# }
#
# Also optionally store your resume text or base cover letter in config.json,
# or keep them in separate files. For demonstration, we'll just store the key.

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

openai.api_key = config["api_key"]

# ----------------------------------------------------------------------------
# 2. (Optional) Use a function to generate or refine a cover letter using OpenAI
# ----------------------------------------------------------------------------

def generate_cover_letter(job_title, company_name, job_description, base_resume_text):
    """
    Using OpenAI to generate a cover letter or summary snippet
    tailored to a specific job listing.
    
    job_title: Title of the position (e.g. 'Software Engineer')
    company_name: Name of the company
    job_description: Full or partial job description text
    base_resume_text: The text of your resume or relevant experience

    Return: A string containing your AI-generated cover letter (or snippet)
    """
    prompt = (
        f"Write a short, professional cover letter for a {job_title} position at {company_name}. "
        f"Here is the job description:\n\n{job_description}\n\n"
        "Here is my relevant experience:\n"
        f"{base_resume_text}\n\n"
        "The cover letter should be concise, highlighting how my experience matches the job requirements. "
        "Do not include placeholder text. Be direct and clear."
    )

    try:
        response = openai.ChatCompletion.create(
            model="o3-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=350,
        )
        cover_letter = response.choices[0].message.content.strip()
        return cover_letter
    except Exception as e:
        print("Error calling OpenAI:", str(e))
        return "Error generating cover letter"

# ----------------------------------------------------------------------------
# 3. Selenium Setup
# ----------------------------------------------------------------------------
# This function initializes Chrome with an updated ChromeDriver.  
# In your case, you run update.ps1 from a separate script/cmd before launching.
#
# For instance, from a Windows CMD you might do:
#   PowerShell.exe -ExecutionPolicy Bypass -File update.ps1
# Then run this Python script.

def create_webdriver(chromedriver_path=None, headless=False):
    """
    Create and return a Chrome WebDriver instance. 
    - If chromedriver_path is None, assume it's on PATH or in the same folder.
    - If headless=True, runs Chrome in headless mode (no visible browser).
    """
    options = webdriver.ChromeOptions()

    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

    if chromedriver_path is None:
        # If update.ps1 places chromedriver in a known location:
        # e.g., "AppData/Local/Programs/Python/Python312/Scripts/chromedriver.exe"
        # Just rely on PATH or that location. 
        service = Service()
    else:
        service = Service(chromedriver_path)

    driver = webdriver.Chrome(service=service, options=options)
    driver.maximize_window()
    return driver

# ----------------------------------------------------------------------------
# 4. Example Function to Apply to a Job
# ----------------------------------------------------------------------------

def apply_to_job(
    driver,
    job_url,
    full_name,
    email,
    phone,
    resume_path,
    cover_letter_text=None
):
    """
    Navigate to a job posting, fill in the fields, and submit the application.
    Customize your By selectors, XPaths, or CSS selectors to match the target site.
    """

    # 4A. Go to the job listing page
    driver.get(job_url)
    time.sleep(3)  # Wait for the page to load; adjust as needed

    # 4B. Find the 'Apply' or 'Apply Now' button
    # Example XPATH or CSS. This is site-specific.
    # Adjust accordingly, e.g.:
    # apply_button = driver.find_element(By.XPATH, "//button[@id='apply-btn']")
    # We'll do a placeholder:
    try:
        apply_button = driver.find_element(By.XPATH, "//button[contains(text(),'Apply')]")
        apply_button.click()
    except Exception as e:
        print("Could not find 'Apply' button:", e)
        return

    time.sleep(2)

    # 4C. Fill out form fields
    # These selectors are placeholders; replace with the actual form fields on your site.
    try:
        name_field = driver.find_element(By.NAME, "name")
        name_field.clear()
        name_field.send_keys(full_name)

        email_field = driver.find_element(By.NAME, "email")
        email_field.clear()
        email_field.send_keys(email)

        phone_field = driver.find_element(By.NAME, "phone")
        phone_field.clear()
        phone_field.send_keys(phone)
    except Exception as e:
        print("Error filling personal info fields:", e)
        return

    time.sleep(1)

    # 4D. Upload Resume
    # Typically: input type="file"
    try:
        resume_upload = driver.find_element(By.NAME, "resume")
        resume_upload.send_keys(resume_path)
    except Exception as e:
        print("Error uploading resume:", e)
        return

    time.sleep(1)

    # 4E. Fill or Upload Cover Letter (if required)
    if cover_letter_text:
        # Possibly there's a text area for cover letter
        try:
            cover_letter_field = driver.find_element(By.NAME, "cover_letter")
            cover_letter_field.clear()
            cover_letter_field.send_keys(cover_letter_text)
        except Exception as e:
            print("Error filling cover letter:", e)
            # Some sites might have a file upload for cover letter
            # cover_letter_upload = driver.find_element(By.ID, "cover_letter_upload")
            # cover_letter_upload.send_keys("C:\\path\\to\\cover_letter.docx")
            pass

    time.sleep(1)

    # 4F. Submit the application
    try:
        submit_button = driver.find_element(By.CSS_SELECTOR, "button.submit")
        submit_button.click()
    except Exception as e:
        print("Error submitting application:", e)
        return

    # 4G. Optionally wait for confirmation
    time.sleep(5)

    # You might check for a success message or confirmation
    # Example:
    # confirmation_text = driver.find_element(By.CLASS_NAME, "confirmation").text
    # print("Submission confirmed:", confirmation_text)

    print("Application submitted successfully for:", job_url)

# ----------------------------------------------------------------------------
# 5. Main Routine / Example Usage
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    # Example usage:
    
    # 5A. Ensure you have run update.ps1 to update ChromeDriver, or do so manually:
    # os.system('powershell.exe -ExecutionPolicy Bypass -File update.ps1')
    # (Uncomment the line above if you want Python to call your PowerShell script.)
    
    # 5B. Create a WebDriver instance
    driver = create_webdriver(
        chromedriver_path=None,  # or specify exact path if needed
        headless=False  # Set True if you want no visible browser
    )

    # 5C. Generate a short cover letter from OpenAI
    job_title = "Frontend Developer"
    company_name = "TechStartup Inc."
    job_description = (
        "We are looking for a Frontend Developer with React experience. "
        "Strong CSS, JavaScript, and teamwork skills are needed. Knowledge of TypeScript is a plus."
    )
    base_resume_text = (
        "I have 4 years of experience in building web applications using React, Redux, and TypeScript. "
        "I also have a strong background in CSS and design systems."
    )

    cover_letter = generate_cover_letter(job_title, company_name, job_description, base_resume_text)

    # 5D. Apply to the job using Selenium
    # Replace the placeholders below with real info and a real job posting URL.
    apply_to_job(
        driver=driver,
        job_url="https://www.example-job-board.com/jobs/frontend-developer-12345",
        full_name="John Doe",
        email="john.doe@example.com",
        phone="123-456-7890",
        resume_path=r"C:\path\to\JohnDoe_Resume.pdf",
        cover_letter_text=cover_letter
    )

    # 5E. Quit the browser once done
    driver.quit()
    print("Done.")