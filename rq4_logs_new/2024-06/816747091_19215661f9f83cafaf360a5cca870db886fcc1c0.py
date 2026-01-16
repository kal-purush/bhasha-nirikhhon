from dataclasses import dataclass

@dataclass
class AnimalClass:
    name: str
    habitat: str
    teeth: int = 0

snowman = AnimalClass('yeti', 'Himalayas', 46)
print(snowman)
print(snowman.habitat)
# Output:
#  AnimalClass(name='yeti', habitat='Himalayas', teeth=46)
#  Himalayas

duck = AnimalClass(habitat='lake', name='duck')
print(duck)
print(duck.habitat)
# Output:
#  AnimalClass(name='duck', habitat='lake', teeth=0)
#  lake

duck = AnimalClass(habitat='lake')
# Error:
#  Traceback (most recent call last):
#  File "/Users/ryo/Desktop/til/Python/data_class.py", line 16, in <module>
#    duck = AnimalClass(habitat='lake')
#  TypeError: __init__() missing 1 required positional argument: 'name'