import sys

def evaluate_expression(expression):
    expression = expression.replace(" ", "")  # Supprimer les espaces de l'expression
    tokens = tokenize(expression)
    result = evaluate(tokens)
    return result

def tokenize(expression):
    tokens = []
    i = 0

    while i < len(expression):
        char = expression[i]

        if char.isdigit() or char == '.':
            num = char
            i += 1
            while i < len(expression) and (expression[i].isdigit() or expression[i] == '.'):
                num += expression[i]
                i += 1
            tokens.append(float(num))
        elif char in {'+', '-', '*', '/', '%', '(', ')'}:
            tokens.append(char)
            i += 1
        else:
            sys.exit(f"Erreur : symbole non reconnu dans l'expression : {char}")

    return tokens

def evaluate(tokens):
    operators = {'+': 1, '-': 1, '*': 2, '/': 2, '%': 2}
    stack = []
    operator_stack = []

    def apply_operator():
        operator = operator_stack.pop()
        operand2 = stack.pop()
        operand1 = stack.pop()
        if operator == '+':
            stack.append(operand1 + operand2)
        elif operator == '-':
            stack.append(operand1 - operand2)
        elif operator == '*':
            stack.append(operand1 * operand2)
        elif operator == '/':
            stack.append(operand1 / operand2)
        elif operator == '%':
            stack.append(operand1 % operand2)

    i = 0
    while i < len(tokens):
        token = tokens[i]

        if isinstance(token, (int, float)):
            stack.append(token)
        elif token in operators:
            while operator_stack and operator_stack[-1] in operators and operators[token] <= operators[operator_stack[-1]]:
                apply_operator()
            operator_stack.append(token)
        elif token == '(':
            operator_stack.append(token)
        elif token == ')':
            while operator_stack[-1] != '(':
                apply_operator()
            operator_stack.pop()  # pop '('
        else:
            sys.exit(f"Erreur : symbole non reconnu dans l'expression : {token}")

        i += 1

    while operator_stack:
        apply_operator()

    if len(stack) != 1:
        sys.exit("Erreur : l'expression n'est pas valide.")

    return stack[0]

def main():
    if len(sys.argv) != 2:
        sys.exit("Usage : python feu02.py \"expression_arithmétique\"")

    expression = sys.argv[1]
    result = evaluate_expression(expression)
    print(result)

if __name__ == "__main__":
    main()

