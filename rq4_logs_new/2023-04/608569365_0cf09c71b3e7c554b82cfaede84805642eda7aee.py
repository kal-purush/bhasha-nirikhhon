# <HW12> Task 2
class MyContextManager:
    """
    Context manager that prints "==========" before and after the code.
    Displays errors if occurred without exiting program
    """
    def __enter__(self):
        print("==========")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Error occurred: {exc_type} {exc_val}")

        print("==========")
        return True


if __name__ == "__main__":
    some_list = []
    with MyContextManager():
        print(some_list)
        print(some_list[1])

    with MyContextManager():
        print(10/0)

    print("Program finished ok!")