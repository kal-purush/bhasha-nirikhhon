import os

class Utility:
    @staticmethod
    def clear_screen():
        """Clears the terminal screen."""
        os.system("cls")

    @staticmethod
    def display_header(title):
        """Displays a formatted header."""
        print("=" * 35)
        print(f"{title.center(35)}")
        print("=" * 35)

    @staticmethod
    def divider():
        """Prints a divider line."""
        print("-" * 35)

    @staticmethod
    def display_menu(title=None, options=[]):
        """Displays a menu with options."""
        if title:  # Only display header if title is provided
            Utility.display_header(title)
        
        for i, option in enumerate(options, 1):
            print(f"[{i}] {option}")
        Utility.divider()
        return input("Select an option: ")