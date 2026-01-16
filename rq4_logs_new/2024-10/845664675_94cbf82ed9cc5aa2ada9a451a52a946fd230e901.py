"""Practice with dictionary utility functions."""

__author__ = "730670853"


def invert(original: dict[str, str]) -> dict[str, str]:
    """Inverting the values of a dictionary of strings."""
    inversion: dict[str, str] = {}
    for key in original:  # for loop to iterate through all elements/check for errors
        value = original[key]  # step one of inverting the elements
        if value in inversion:
            raise KeyError(
                f"'{value}' is a duplicate value!"
            )  # make sure keys are identical
        inversion[value] = key  # step two of inverting the elements
    return inversion


def favorite_color(namecolor: dict[str, str]) -> str:
    """Finding the color that appears the most frequently."""
    colors: list[str] = []  # making a list to store color names
    colorcount: dict[str, int] = {}  # a dictionary with colors and color counts
    max: str = ""  # most popular color
    maxcount: int = 0  # frequency of color appearance
    for name in namecolor:
        colors.append(namecolor[name])  # adding all the colors to the list color
    for color in colors:
        if color in colorcount:
            colorcount[
                color
            ] += 1  # these next lines assigning values to the colors in the dict
        else:
            colorcount[color] = 1
    for (
        color
    ) in (
        colorcount
    ):  # iterating through the colors to find the color with highest value
        if colorcount[color] > maxcount:
            maxcount = colorcount[color]
            max = color
    return max


def count(inputlist: list[str]) -> dict[str, int]:
    """Finding the amount of occurrences of a string value in a list."""
    output: dict[str, int] = {}
    for elem in inputlist:  # iterating through every inputlist element
        if elem in output:  # considering the same element but with respect to output
            output[elem] += 1
        else:
            output[elem] = 1
    return output


def alphabetizer(alphabet: list[str]) -> dict[str, list[str]]:
    """Creating a list with keys of the first letters of each word."""
    letterlist: dict[str, list[str]] = {}  # empty dictionary for entries
    for elem in alphabet:
        firstletter = elem[0].lower()  # finding the first letter of every word
        if (
            firstletter in letterlist
        ):  # if/else to ensure each key has all words with that value
            letterlist[firstletter].append(elem)
        else:
            letterlist[firstletter] = [elem]
    return letterlist


def update_attendance(
    attendancelist: dict[str, list[str]], day: str, student: str
) -> None:
    """Figuring out who attended class each day."""
    if (
        day in attendancelist
    ):  # Adding students to the respective day they attended class
        if student in attendancelist[day]:
            return None
        else:
            attendancelist[day].append(student)
    else:
        attendancelist[day] = [student]
    return None