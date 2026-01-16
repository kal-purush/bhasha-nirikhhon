from tiny_storage import Unit, Type

config = Unit('tstest')

print("Hello", config('greeting').put(input))