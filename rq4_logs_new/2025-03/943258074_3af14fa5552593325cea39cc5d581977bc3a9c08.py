import time
import job_system
from user import User
import sqlite3
from utils import Utility

class Employer(User):
    def __init__(self, id, username, password, role, name=None, skills=None, experience=None, hourly_rate=None, payment_method=None, company_name=None):
        super().__init__(id, username, password, role)  # Pass common attributes to the parent class
        self.company_name = company_name
        self.posted_jobs = []  # List to store jobs posted by the employer

    def post_job(self, title, description, budget, skill_required, duration):
        """Creates a new job posting, adds it to the database, and returns the Job object."""
       
        # Create Job object in memory
        job = job_system.Job(title, description, budget, skill_required, duration, [])

        # Store job in database
        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO jobs (employer_id, title, description, budget, skills_required, duration, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        """, (self.id, title, description, budget, skill_required, duration))

        # Get the ID of the newly inserted job
        job_id = cursor.lastrowid
        job.id = job_id  # Assign the ID to the job object

        conn.commit()
        conn.close()

        # Add job to employer's posted jobs list
        self.posted_jobs.append(job)

        return job  # Returning the job instance

    def view_applicants(self, job_title):
        """Displays applicants for a job posted by the employer."""

        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()

        # Check if the employer has posted this job
        cursor.execute("SELECT id FROM jobs WHERE title = ? AND employer_id = ?", (job_title, self.id))
        job_data = cursor.fetchone()

        if not job_data:
            print(f"You haven't posted a job titled '{job_title}'.")
            conn.close()
            return

        job_id = job_data[0]  # Extract job ID

        # Fetch applicants for this job
        cursor.execute("""
            SELECT u.id, u.name, u.skills, u.experience, u.hourly_rate, ja.id
            FROM users u
            JOIN job_applications ja ON u.id = ja.freelancer_id
            WHERE ja.job_id = ? AND ja.status = 'applied'
        """, (job_id,))

        applicants = cursor.fetchall()
        conn.close()

        if not applicants:
            print(f"No applicants for the job '{job_title}' yet.")
            return

        # Manage applicants
        while True:
            # Display applicants
            Utility.clear_screen()
            Utility.display_header(f"Applicants for {job_title}")
            for freelancer_id, name, skills, experience, hourly_rate, application_id in applicants:
                print(f"  Applicant: {name}")  # <-- Freelancer Name instead of ID
                print(f"  Skills: {skills}")
                print(f"  Experience: {experience}")
                print(f"  Hourly Rate: ${hourly_rate:.2f}/hr")
                Utility.divider()
            action = input("Enter Applicant Name or type 'exit': ").strip()

            if action.lower() == "exit":
                break

            applicant = next((app for app in applicants if app[1].lower() == action.lower()), None)

            if not applicant:
                print("\nInvalid Freelancer Name. Try again.")
                time.sleep(1.5)
                continue

            _, name, skills, experience, hourly_rate, application_id = applicant
            self.manage_applicant(application_id, name, skills, experience, hourly_rate)

    def manage_applicant(self, application_id, name, skills, experience, hourly_rate):
        """Allows the employer to view the applicant profile and accept/reject them."""

        Utility.clear_screen()
        Utility.display_header(f"{name}'s Profile")
        print(f"  Skills: {skills}")
        print(f"  Experience: {experience}")
        print(f"  Hourly Rate: ${hourly_rate:.2f}/hr")
        Utility.divider()

        # Accept or Reject
        while True:
            decision = input("Accept (A) or Reject (R) this applicant? ").strip().lower()
            if decision in ["a", "r"]:
                new_status = "accepted" if decision == "a" else "rejected"
                print(f"\n{name} has been {'accepted' if decision == 'a' else 'rejected'}.")
                time.sleep(1.5)

                # Update database
                conn = sqlite3.connect("freelancer_marketplace.db")
                cursor = conn.cursor()
                cursor.execute("UPDATE job_applications SET status = ? WHERE id = ?", (new_status, application_id))
                conn.commit()
                conn.close()
                break
            else:
                print("Invalid input. Please enter 'A' to accept or 'R' to reject.\n")
       
    def print_posted_jobs(self):
        """Fetch and print all jobs posted by the employer."""
        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, title, description, budget, skills_required, duration, status
            FROM jobs
            WHERE employer_id = ?
        """, (self.id,))

        jobs = cursor.fetchall()
        conn.close()

        if not jobs:
            print("You have not posted any jobs yet.")
            return

        Utility.clear_screen()
        Utility.display_header("Your Posted Jobs")
        for job in jobs:
            job_id, title, description, budget, skills, duration, status = job
            print(f"Job ID: {job_id}\nTitle: {title}\nDescription: {description}\nBudget: ${budget}\nSkills Required: {skills}\nDuration: {duration}\nStatus: {status}")
            Utility.divider()