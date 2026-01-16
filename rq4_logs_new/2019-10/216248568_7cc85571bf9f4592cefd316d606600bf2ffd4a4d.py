class Person(object):
    def __init__(self,id,height,weight,age,gender,**kwargs):
        self.id = str(id)
        self.age = int(age)
        self.weight = float(weight)
        self.height = float(height)
        self.age = int(age)
        self.gender = str(gender)
    def __str__(self):
        return f"{self.id} ({self.age}), height:{self.height}, weight: {self.weight}"

class Patient(Person):
    def __init__(self,id,height,weight,age,gender,hospital_id,cause,**kwargs):
        self.cause = str(cause)
        self.hospital_id = str(hospital_id)
        super().__init__(id,height,weight,age,gender)

    def __str__(self):
        return super().__str__() + f"\n hospital:{self.hospital_id}, cause:{self.cause}"
