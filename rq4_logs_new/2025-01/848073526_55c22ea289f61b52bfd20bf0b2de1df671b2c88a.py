def greet(name):
    print "Hello " + name  # Intentional bug: missing parentheses
    return None  # Unnecessary return

for i in range(1000000):
    greet("world")  # Performance issue to test review