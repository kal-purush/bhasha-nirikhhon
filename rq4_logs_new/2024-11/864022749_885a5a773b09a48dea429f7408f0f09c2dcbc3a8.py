"""
Time: 9:15 pm
Time taken: 17 mins
"""
class ProgrammingLanguage:
    """Represent a programming language with typing, reflection support, and year created."""

    def __init__(self, name, typing, reflection, year):
        """Initialize a ProgrammingLanguage instance with name, typing, reflection, and year."""
        self.name = name
        self.typing = typing
        self.reflection = reflection
        self.year = year

    def is_dynamic(self):
        """Return True if the language is dynamically typed, False otherwise."""
        return self.typing.lower() == "dynamic"

    def __str__(self):
        """Return a string representation of a ProgrammingLanguage object."""
        return f"{self.name}, {self.typing} Typing, Reflection={self.reflection}, First appeared in {self.year}"

def demo_programming_languages():
    """Demonstrate the use of the ProgrammingLanguage class with sample languages."""
    ruby_language = ProgrammingLanguage("Ruby", "Dynamic", True, 1995)
    python_language = ProgrammingLanguage("Python", "Dynamic", True, 1991)
    vb_language = ProgrammingLanguage("Visual Basic", "Static", False, 1991)

    language_samples = [ruby_language, python_language, vb_language]

    print(python_language)

    print("The dynamically typed languages are:")
    for lang in language_samples:
        if lang.is_dynamic():
            print(lang.name)


if __name__ == "__main__":
    demo_programming_languages()