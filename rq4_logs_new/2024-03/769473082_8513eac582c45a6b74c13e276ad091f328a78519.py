def calculate(num1, num2, operator):
  """
  Performs basic arithmetic operations based on the provided operator.

  Args:
      num1: First number.
      num2: Second number.
      operator: The arithmetic operation to perform (+, -, *, /).

  Returns:
      The result of the calculation.
  """
  if operator == "+":
    return num1 + num2
  elif operator == "-":
    return num1 - num2
  elif operator == "*":
    return num1 * num2
  elif operator == "/":
    if num2 == 0:
      print("Error: Division by zero")
      return None  # Indicate error
    else:
      return num1 / num2
  else:
    print("Invalid operator")
    return None  # Indicate error

while True:
  # Get user input
  try:
    num1 = float(input("Enter first number: "))
    num2 = float(input("Enter second number: "))
    operator = input("Choose operation (+, -, *, /): ")
  except ValueError:
    print("Invalid input. Please enter numbers only.")
    continue

  # Perform calculation
  result = calculate(num1, num2, operator)

  # Display result
  if result is not None:
    print(f"{num1} {operator} {num2} = {result}")

  # Ask if user wants to continue
  choice = input("Do you want to perform another calculation? (y/n): ").lower()
  if choice != "y":
    break

print("Calculator closed.")