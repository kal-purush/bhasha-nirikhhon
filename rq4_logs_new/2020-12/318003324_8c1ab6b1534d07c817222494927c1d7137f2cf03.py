
class Expense():
    def __init__(self, title, amount, created_on, tags):
        self.title = str(title)
        self.amount = float(amount)
        self.created_on = created_on
        self.tags = tags

    def __repr__(self):
        return "{self.__class__.__name__}({self.title}, {self.amount}, {self.created_on}, {self.tags})".format(self=self)

    def __str__(self):
        return "Title: {self.title}, Amount:  {self.amount}, Created_On:  {self.created_on}, Tags: {self.tags}".format(self=self)


if __name__ == "__main__":
    exp = Expense("target", 45, "12/06/20", ["shopping", "bills"])
    print(exp)
    print(exp.__str__())
    print(exp.__repr__())

    print("Title:", exp.title)
    print("Amount:", exp.amount)
    print("Created_On:", exp.created_on)
    print("Tags:", exp.tags)