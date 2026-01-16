# Given list of tuples (name, surname, age, profession, City location)
# 1 - Add your new record o the beginning of the given list
# 2 - In modified list swap elements with indexes 1 and 5 (1<->5). Print result
# 3 - check that all people in modified list with records indexes 6, 10, 13
#   have age >=30. Print condition check result

people_records = [

  ('John', 'Doe', 28, 'Engineer', 'New York'),
  ('Alice', 'Smith', 35, 'Teacher', 'Los Angeles'),
  ('Bob', 'Johnson', 45, 'Doctor', 'Chicago'),
  ('Emily', 'Williams', 30, 'Artist', 'San Francisco'),
  ('Michael', 'Brown', 22, 'Student', 'Seattle'),
  ('Sophia', 'Davis', 40, 'Lawyer', 'Boston'),
  ('David', 'Miller', 33, 'Software Developer', 'Austin'),
  ('Olivia', 'Wilson', 27, 'Marketing Specialist', 'Denver'),
  ('Daniel', 'Taylor', 38, 'Architect', 'Portland'),
  ('Grace', 'Moore', 25, 'Graphic Designer', 'Miami'),
  ('Samuel', 'Jones', 50, 'Business Consultant', 'Atlanta'),
  ('Emma', 'Hall', 31, 'Chef', 'Dallas'),
  ('William', 'Clark', 29, 'Financial Analyst', 'Houston'),
  ('Ava', 'White', 42, 'Journalist', 'San Diego'),
  ('Ethan', 'Anderson', 36, 'Product Manager', 'Phoenix')
]

#1
new_record_1 = ('Oleg', 'Petrov', 32, 'Human', 'Kharkiv')
people_records.insert(0, new_record_1)
print(people_records)

#2
index_fifth = people_records.pop(5)
index_first = people_records.pop(1)
people_records.insert(1, index_fifth)
people_records.insert(5, index_first)
print(people_records)

#3
index_sixth = people_records.pop(6)
index_tenth = people_records.pop(10)
index_eleventh = people_records.pop(11)

if index_sixth[2] >=30:
  print (index_sixth[0] + " " + index_sixth[1] + " is older than 30 y. ")

if index_tenth[2] >=30:
  print(index_tenth[0] + " " + index_tenth[1] + " is older than 30 y. ")

if index_eleventh[2] >= 30:
  print(index_eleventh[0] + " " + index_eleventh[1] + " is older than 30 y. ")