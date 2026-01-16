import csv

class Education():
    def __init__(self, name, delta_knowledge_level, delta_happiness_level):
        self.name = name
        self.delta_knowledge_level = delta_knowledge_level
        self.delta_happiness_level = delta_happiness_level


    @classmethod
    def create_by_csv(cls, file_name):
        education_list = []
        with open(file_name, "r") as f:
            reader = csv.reader(f, delimiter=",")
            for education in reader:
                education_list.append(Education(education[0], education[1], education[2]))
        return education_list

file_name = "data/education.csv"