# Task 1: Read a File and Handle Errors

try:
    print("Reading file content:\n")
    file = open("..\content.txt", "r")

    line_number = 1
    for line in file:
        print(f"Line {line_number}: {line.strip()}")
        line_number += 1

    file.close()
except FileNotFoundError:
    print("Error: The file 'content.txt' was not found.")
except Exception as e:
    print("An unexpected error occurred:", str(e))