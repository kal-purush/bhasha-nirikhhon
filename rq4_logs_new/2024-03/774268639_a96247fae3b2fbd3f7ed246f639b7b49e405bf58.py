def generate_schedule(objects, iterations):
    # Sort objects by their position to ensure correct order
    objects_sorted = sorted(objects, key=lambda x: x[2])

    # Initialize the schedule table
    schedule = []

    # Calculate the start and end time for each object in each iteration
    for i in range(iterations):
        current_time = 0  # Reset current time for each iteration
        iteration_schedule = []
        for obj in objects_sorted:
            name, required_time, position = obj
            start_time = current_time
            end_time = start_time + required_time
            iteration_schedule.append((name, start_time, end_time))
            current_time += required_time  # Update current time for the next object

        # Add the iteration's schedule to the overall schedule
        schedule.append(iteration_schedule)

    # Print the schedule
    for i, iteration_schedule in enumerate(schedule):
        print(f"Iteration {i + 1}:")
        for entry in iteration_schedule:
            name, start, end = entry
            print(f"  {name}: {start}-{end} seconds")
        print()


# Example usage
objects = [
    ("BAS", 5, 15),  # Example object
    ("NFBREG", 10, 30),
    ("CHOICE", 5, 6),
    ("rank1", 5, 6),  # As specified
    ("rank2", 5, 6)
]

generate_schedule(objects, 2)  # For 2 iterations