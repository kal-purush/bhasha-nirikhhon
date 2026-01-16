def calc_fib(n):
    """
    Poorly implemented function to calculate the nth Fibonacci number
    """       
    if n < 2:
        return n
    
    # Note the recursion here
    return calc_fib(n-2) + calc_fib(n-1)

def print_fib(n):    
    """
    Prints the nth Fibonacci number
    """
    print(calc_fib(n))