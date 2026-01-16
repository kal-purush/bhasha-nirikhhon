#Define the Data structure, this will allow each task to be a string
task = []

#display menu options for the user to interact with the to do list.
while True:
    print("To-Do List:")
    print("1. Add task")
    print("2. View tasks")
    print("3. Exit")
    
    choice = input("Enter your choice: ")
    
    if choice == "1":
        # Add task
        task = input("Enter the task: ")
        tasks.append(task)
        print("Task added!")
        pass
    elif choice == "2":
        # View tasks
        print("Tasks:")
        for index, task in enumerate(tasks, start=1):
        print(f"{index}. {task}")
        pass
    elif choice == "3":
        print("Goodbye!")
        break
    else:
        print("Invalid choice. Please choose again.")