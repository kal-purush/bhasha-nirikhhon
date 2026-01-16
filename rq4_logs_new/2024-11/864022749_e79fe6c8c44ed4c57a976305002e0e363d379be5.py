import datetime


class Project:
    """Represents a project with attributes such as name, start date, priority, cost, and completion percentage."""

    def __init__(self, name, start_date, priority, cost_estimate, completion):
        # Check if start_date is already a datetime object; if not, convert it.
        if isinstance(start_date, str):
            self.start_date = datetime.strptime(start_date, "%d/%m/%Y").date()
        else:
            self.start_date = start_date
        self.name = name
        self.priority = int(priority)
        self.cost_estimate = float(cost_estimate)
        self.completion = int(completion)

    def __str__(self):
        return f"{self.name}, start: {self.start_date.strftime('%d/%m/%Y')}, priority {self.priority}, estimate: ${self.cost_estimate:,.2f}, completion: {self.completion}%"

    def is_complete(self):
        return self.completion == 100

    def update(self, completion=None, priority=None):
        if completion is not None:
            self.completion = int(completion)
        if priority is not None:
            self.priority = int(priority)

    def __lt__(self, other):
        """Comparison method to allow sorting by priority."""
        return self.priority < other.priority

    def starts_after(self, date):
        """Checks if the project starts after a given date."""
        return self.start_date > date