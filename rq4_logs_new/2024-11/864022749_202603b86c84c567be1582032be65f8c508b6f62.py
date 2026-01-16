import datetime
from prac_07.project import Project

MENU = "- (L)oad projects\n- (S)ave projects\n- (D)isplay projects\n- (F)ilter projects by date\n- (A)dd new project\n- (U)pdate project\n- (Q)uit"


def main():
    print("Welcome to Pythonic Project Management")
    projects = load_projects("projects.txt")
    print(f"Loaded {len(projects)} projects from projects.txt")

    choice = input(MENU + "\n>>> ").lower()
    while choice != 'q':
        if choice == 'l':
            filename = input("Filename: ")
            projects = load_projects(filename)
        elif choice == 's':
            filename = input("Filename: ")
            save_projects(filename, projects)
        elif choice == 'd':
            display_projects(projects)
        elif choice == 'f':
            filter_projects_by_date(projects)
        elif choice == 'a':
            add_new_project(projects)
        elif choice == 'u':
            update_project(projects)
        else:
            print("Invalid choice")

        choice = input(MENU + "\n>>> ").lower()

    if input("Would you like to save to projects.txt? (y/n): ").lower() == 'y':
        save_projects("projects.txt", projects)
    print("Thank you for using custom-built project management software.")


def load_projects(filename):
    """Load projects from a file."""
    projects = []
    with open(filename, 'r') as file:
        next(file)
        for line in file:
            parts = line.strip().split('\t')
            name = parts[0]
            start_date = datetime.strptime(parts[1], "%d/%m/%Y").date()
            priority = int(parts[2])
            cost_estimate = float(parts[3])
            completion = int(parts[4])
            project = Project(name, start_date, priority, cost_estimate, completion)
            projects.append(project)
    return projects


def save_projects(filename, projects):
    """Save projects to a file."""
    with open(filename, 'w') as file:
        file.write("Name\tStart Date\tPriority\tCost Estimate\tCompletion\n")
        for project in projects:
            file.write(
                f"{project.name}\t{project.start_date.strftime('%d/%m/%Y')}\t{project.priority}\t{project.cost_estimate}\t{project.completion}\n")


def display_projects(projects):
    incomplete = [project for project in projects if not project.is_complete()]
    complete = [project for project in projects if project.is_complete()]
    incomplete.sort()
    complete.sort()

    print("\nIncomplete projects:")
    for project in incomplete:
        print(" ", project)

    print("\nCompleted projects:")
    for project in complete:
        print(" ", project)


def filter_projects_by_date(projects):
    """Display projects that start after a user-specified date."""
    date_string = input("Show projects that start after date (dd/mm/yyyy): ")
    filter_date = datetime.datetime.strptime(date_string, "%d/%m/%Y").date()
    filtered_projects = [project for project in projects if project.starts_after(filter_date)]
    filtered_projects.sort(key=lambda p: p.start_date)

    print("\nFiltered projects:")
    for project in filtered_projects:
        print(" ", project)


def add_new_project(projects):
    """Add a new project based on user input."""
    print("Let's add a new project")
    name = input("Name: ")
    start_date = input("Start date (dd/mm/yyyy): ")
    priority = int(input("Priority: "))
    cost_estimate = float(input("Cost estimate: "))
    completion = int(input("Percent complete: "))
    start_date = datetime.datetime.strptime(start_date, "%d/%m/%Y").date()
    projects.append(Project(name, start_date, priority, cost_estimate, completion))


def update_project(projects):
    """Update an existing project's completion and priority."""
    for i, project in enumerate(projects):
        print(f"{i} {project}")

    project_index = int(input("Project choice: "))
    project = projects[project_index]
    print(project)

    completion = input("New Percentage: ")
    priority = input("New Priority: ")

    project.update(int(completion) if completion else None, int(priority) if priority else None)


main()