def quadratic():

    while True:
        try:   
            a = int(input('A: '))
            b = int(input('B: '))
            c = int(input('C: '))

            x1 = (-b + (b**2 - 4*a*c)**0.5)/(2*a)
            x2 = (-b - (b**2 - 4*a*c)**0.5)/(2*a) 

        except ValueError:
            print('Only numbers are accepted. Please enter a number')
            continue
            
        except ZeroDivisionError:
            break
        
        else:
            break
               
    if a==0 and b==0:
        print('This is just a constant')
        
    elif a==0:
        print('This is a Linear Equation')
        
    elif b**2 - 4*a*c == 0:
        print('Roots are Perfect Square')
        print(f'The roots of {a}x^2 + {b}x + {c} = 0 are ({x1},{x2})')
        
    elif b**2 - 4*a*c > 0:
        print('Roots are Distinct and Real ')
        print(f'The roots of {a}x^2 + {b}x + {c} = 0 are ({x1},{x2})')
        
    elif b**2 - 4*a*c < 0:
        print('Roots are Imaginary')
        print(f'The roots of {a}x^2 + {b}x + {c} = 0 are ({x1},{x2})')