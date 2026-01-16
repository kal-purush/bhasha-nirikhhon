def value_error_exception_handler(value_type=int,input_message="Please enter a number: " ,error_message="Invalid input. Please enter a number.", max_value=None, min_value=None):
    while True:
        try: 
            user_input = value_type(input(input_message))
            if min_value is not None and user_input < min_value:
                print(f"Input is below the minimum allowed value of {min_value}.")
                continue
            if max_value is not None and user_input > max_value:
                print(f"Input exceeds the maximum allowed value of {max_value}.")
                continue
        except ValueError:
            print(error_message)
        else:
            break
    return user_input
