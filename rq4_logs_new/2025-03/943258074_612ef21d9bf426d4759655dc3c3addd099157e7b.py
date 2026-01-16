import sqlite3
import os
import time
from user import User
from utils import Utility
import job_system
import freelancer_marketplace

# Database Setup
def init_db():
    db_path = os.path.abspath("freelancer_marketplace.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT UNIQUE,
                        password TEXT,
                        role TEXT,
                        name TEXT,
                        skills TEXT,
                        experience TEXT,
                        hourly_rate REAL,
                        payment_method TEXT,
                        company_name TEXT
                    )''')

    # Recreate the table with the correct schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS temporary_wallet (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employer_id INTEGER NOT NULL,
            freelancer_id INTEGER NOT NULL,
            balance REAL DEFAULT 0.0,
            FOREIGN KEY (freelancer_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (employer_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    conn.close()


def freelancer_menu(user):
    """Handles freelancer actions like browsing jobs and tracking applications."""
    while True:
        Utility.clear_screen()  # Clear the terminal only once at the start of the loop
        choice = Utility.display_menu(f"{user.role} Menu", ["Browse and Apply Jobs", "Track Applications", "Work in Progress", "Wallet & Payment","Edit Profile", "Logout"], use_header=True)

        if choice == "1":
            user.browse_jobs()  # Display jobs without clearing the terminal again
        elif choice == "2":
            user.track_applications()
        elif choice == "3":
            job_id = select_job(user)
            if job_id:
                progress_display(user, job_id)
        elif choice == "4":
            user.wallet_menu()
        elif choice == "5":
            user.view_and_edit_profile()
        elif choice == "6":
            user.logout()
            break  # Exit the freelancer dashboard
        else:
            print("Invalid choice! Please try again.")
            time.sleep(1.5)


def employer_menu(user):
    """Handles employer actions like posting jobs and managing applications."""
    while True:
        Utility.clear_screen()  # Clear the terminal
        choice = Utility.display_menu(f"{user.role} Menu", ["Post a Job", "View Posted Jobs", "View Applicants", "Work in Progress","Wallet & Payment Settings", "Logout"], use_header=True)

        if choice == "1":
            # Collect all required job details
            Utility.clear_screen()
            Utility.display_header("Post Job")
            title = input("Enter Job Title: ")
            description = input("Enter Job Description: ")
            budget = float(input("Enter Budget: "))
            skill_required = input("Enter Skills Required (comma-separated): ")
            duration = input("Enter Job Duration (e.g., 1 month): ")
            # Call the post_job method with all required arguments
            user.post_job(title, description, budget, skill_required, duration)
            time.sleep(1.5)
        elif choice == "2":
            user.view_posted_jobs()  # Correct way to call the method
            input("Press Enter to Return...")  # Pause before clearing screen
        elif choice == "3":
            try:
                job_title = input("\nEnter Job Title to View Applicants: ") # Convert to int
                user.view_applicants(job_title)
                input("\nPress Enter to Return...")  # Pause before clearing screen
            except ValueError:
                print("Invalid input! Job ID must be a number.")
                time.sleep(1.5)
        elif choice == "4":
            job_id = select_job(user)  # Ensures the employer selects a job first
            if job_id:
                progress_display(user, job_id)  # Pass selected job_id
            input("\nPress Enter to return to the dashboard...")
        elif choice == "5":
            user.wallet_menu()  # New function for wallet & payment
        elif choice == "6":
            user.logout()
            break  # Exit the Employer dashboard and return to the main menu
        else:
            print("Invalid choice! Please try again.")
            time.sleep(1.5)

def manage_payments(user):
    """Handles employer payments, milestones, and approvals."""
    while True:
        Utility.clear_screen()
        Utility.display_header("Payment Management")
        print("[1] Deposit Funds\n[2] Set Milestones\n[3] Approve Work & Release Payment\n[4] Back")
        choice = input("Select an option: ")

        if choice == "1":
            job_id = input("Enter Job ID to deposit funds: ")
            amount = input("Enter deposit amount: ")
            user.deposit_funds(job_id, amount)
        elif choice == "2":
            job_id = input("Enter Job ID to set milestones: ")
            milestones = input("Enter milestones (comma-separated): ")
            user.set_milestones(job_id, milestones)
        elif choice == "3":
            job_id = input("Enter Job ID to approve work: ")
            user.approve_work_and_release_payment(job_id)
        elif choice == "4":
            break

def select_job(user):
    """Prompts the user to select a job before accessing Work in Progress."""

    conn = sqlite3.connect("freelancer_marketplace.db")
    cursor = conn.cursor()

    if user.role == "Freelancer":
        cursor.execute('''
            SELECT j.id, j.title
            FROM jobs j
            JOIN job_applications ja ON j.id = ja.job_id
            WHERE ja.freelancer_id = ? AND j.status = 'in_progress'
        ''', (user.id,))
    else:  # Employer
        cursor.execute('''
            SELECT id, title
            FROM jobs
            WHERE employer_id = ? AND status IN ('in_progress', 'completed')
        ''', (user.id,))

    jobs = cursor.fetchall()
    conn.close()

    if not jobs:
        print("No active jobs found.")
        time.sleep(1.5)
        return None

    Utility.clear_screen()
    Utility.display_header("Select Job")
    for idx, (job_id, title) in enumerate(jobs, 1):
        print(f"[{idx}] {title}")
    Utility.divider()

    try:
        job_choice = int(input("Enter job number: ")) - 1
        if job_choice < 0 or job_choice >= len(jobs):
            print("Invalid choice.")
            return None
    except ValueError:
        print("Invalid input. Please enter a number.")
        return None

    selected_job_id, selected_title = jobs[job_choice]
    print(f"\nSelected Job: {selected_title}\n")

    return selected_job_id  # Return job_id to pass into progress_display

def progress_display(user, job_id):
    """Displays work in progress for both freelancers and employers, including temporary wallet balance."""

    def display_job_details():
        """Fetch and display job details along with milestones and remaining budget."""
        Utility.clear_screen()
        Utility.display_header("Work in Progress")

        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()

        # Get job details, including budget
        cursor.execute("SELECT title, description, budget, status FROM jobs WHERE id = ?", (job_id,))
        job = cursor.fetchone()

        if not job:
            print("Error: Job not found.")
            conn.close()
            return False

        job_title, job_description, job_budget, job_status = job

        if job_status != "in_progress":
            print("This job is not currently in progress.")
            conn.close()
            return False

        # Calculate total milestone payments
        cursor.execute("SELECT COALESCE(SUM(payment), 0) FROM milestones WHERE job_id = ?", (job_id,))
        total_allocated_budget = cursor.fetchone()[0]

        remaining_budget = job_budget - total_allocated_budget

        print(f"Job: {job_title}\nDescription: {job_description}")
        print(f"Total Budget: Php {job_budget}")
        print(f"Allocated Budget: Php {total_allocated_budget}")
        print(f"Remaining Budget: Php {remaining_budget}\n")

        # Fetch milestones based on user role
        if user.role == "freelancer":
            cursor.execute('''
                SELECT id, title, payment, status
                FROM milestones
                WHERE job_id = ? AND freelancer_id = ?
            ''', (job_id, user.id))
        else:  # Employer
            cursor.execute('''
                SELECT id, title, payment, status
                FROM milestones
                WHERE job_id = ?
            ''', (job_id,))

        milestones = cursor.fetchall()

        if not milestones:
            print("No milestones found for this job.\n")
        else:
            print("\nMilestones:")
            for idx, (milestone_id, title, payment, status) in enumerate(milestones, 1):
                print(f"[{idx}] {title} (Php {payment}) [{status}]")

        # 🟢 Display temporary wallet for freelancers
        if user.role == "freelancer":
            cursor.execute("SELECT balance FROM temporary_wallet WHERE freelancer_id = ?", (user.id,))
            temp_wallet_balance = cursor.fetchone()

            if temp_wallet_balance:
                print(f"\n🟢 Temporary Wallet Balance: Php {temp_wallet_balance[0]}")
            else:
                print("\n🟢 Temporary Wallet Balance: Php 0.00")

        conn.close()
        return True

    while True:
        if not display_job_details():
            break  # Stop loop if job is not in progress or doesn't exist

        if user.role == "Freelancer":
            choice = Utility.display_menu("Options", ["Submit Milestone", "Back"], use_header=False)

            if choice == "1":
                milestone_title = input("Enter milestone title to submit: ").strip()
                user.submit_milestone(milestone_title)  # Submit by title
            elif choice == "2":
                break  # Exit loop
            else:
                print("Invalid choice. Please select a valid option.")

        elif user.role == "Employer":
            choice = Utility.display_menu("Options", ["Add Milestone", "Approve Milestone", "Back"], use_header=False)

            if choice == "1":
                # Before adding, check if budget is enough
                conn = sqlite3.connect("freelancer_marketplace.db")
                cursor = conn.cursor()
                cursor.execute("SELECT budget FROM jobs WHERE id = ?", (job_id,))
                job_budget = cursor.fetchone()[0]

                cursor.execute("SELECT COALESCE(SUM(payment), 0) FROM milestones WHERE job_id = ?", (job_id,))
                total_allocated_budget = cursor.fetchone()[0]

                remaining_budget = job_budget - total_allocated_budget
                conn.close()

                # **NEW CONDITION: Prevent adding a milestone if remaining budget is 0**
                if remaining_budget <= 0:
                    print("Error: No remaining budget available. You cannot add more milestones.")
                    return  # Exit function early

                # Get milestone details
                milestone_title = input("Enter milestone title: ").strip()
                while True:
                    try:
                        milestone_payment = float(input("Enter milestone payment: ").strip())
                        if milestone_payment <= 0:
                            print("Payment must be greater than 0.")
                            continue
                        if milestone_payment > remaining_budget:
                            print(f"Error: Not enough budget. Remaining budget is Php {remaining_budget}")
                            continue
                        break
                    except ValueError:
                        print("Invalid input. Please enter a valid amount.")

                # Add milestone
                user.add_milestone(job_id, milestone_title, milestone_payment)

            elif choice == "2":
                milestone_title = input("Enter milestone title to approve: ").strip()
                user.approve_milestone(milestone_title)
            elif choice == "3":
                break  # Exit loop
            else:
                print("Invalid choice. Please select a valid option.")

        # Add a general exit option for both roles
        if user.role not in ["Freelancer", "Employer"]:
            print(f"DEBUG: User role is '{user.role}'")
            print("Invalid role. Exiting...")
            break


def display_sign_up():
    Utility.clear_screen()
    Utility.display_header("Sign Up")
    username = input("Enter username: ")
    password = input("Enter password: ")

    Utility.clear_screen()
    Utility.display_header("Select Your Role")
    print("[1] Freelancer - Find and apply for jobs\n[2] Employer - Post jobs and hire talent ")
    Utility.divider()
    role = input("Enter role: ")
    User.sign_up(username, password, role)

def display_login():
    Utility.clear_screen()
    Utility.display_header("Login")
    username = input("Enter username: ")
    password = input("Enter password: ")
    return User.login(username, password)

def main():
    init_db()  # Initialize the database
    conn = sqlite3.connect("freelancer_marketplace.db")
    while True:
        Utility.clear_screen()
        Utility.display_header("Welcome to ProDigi")
        print("[1] Sign Up\n[2] Login\n[3] Exit")
        Utility.divider()
        choice = input("Select an option: ")

        if choice == "1":
            display_sign_up()
        elif choice == "2":
            user = display_login()
            if user:
                # Stay in the dashboard until the user logs out
                while True:
                    if user.role == "Freelancer":
                        freelancer_menu(user)  # Enter Freelancer dashboard
                    elif user.role == "Employer":
                        employer_menu(user)  # Enter Employer dashboard
                    else:
                        print("Invalid role! Returning to main menu.")
                        break  # Exit the dashboard loop and return to the main menu
                    # If the user logs out, break the dashboard loop
                    break
        elif choice == "3":
            # Exit
            print("Exiting... Goodbye!")
            break
        else:
            print("\nInvalid choice! Please try again.")
            time.sleep(1.5)
            Utility.clear_screen()

if __name__ == "__main__":
    main()