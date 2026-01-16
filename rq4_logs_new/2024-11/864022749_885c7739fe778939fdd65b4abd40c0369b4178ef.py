from musician import Musician


class Band:
    """Band class representing a musical band."""

    def __init__(self, name):
        """Initialise a Band with a name and empty musicians list."""
        self.name = name
        self.musicians = []

    def __str__(self):
        """Return a string representation of the Band."""
        musician_strs = [str(musician) for musician in self.musicians]
        return f"{self.name} ({', '.join(musician_strs)})"

    def __repr__(self):
        """Return a string representation of the Band for debugging."""
        return f"Band(name={self.name}, musicians={self.musicians})"

    def add(self, musician):
        """Add a musician to the band's list of musicians."""
        self.musicians.append(musician)

    def play(self):
        """Return a string showing the band playing music."""
        return "\n".join([musician.play() for musician in self.musicians])