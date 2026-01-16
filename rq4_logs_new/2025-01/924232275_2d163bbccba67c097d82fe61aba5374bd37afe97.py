def validate_input(prompt, datatype):
    while True:
        try:
            return datatype(input(prompt))
        except ValueError:
            print(f"Invalid input! Please enter a valid {datatype