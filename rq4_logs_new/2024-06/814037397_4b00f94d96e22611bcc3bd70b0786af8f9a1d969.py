########
#Q1
########
def min_enclosing_rectangle(radius, x, y):
    '''
    (num, num, num) -> (num,num) or None if radius is (-)
    When the user inputs 3 values, the function should draw a rectangle that must be a smallest axis-aligned rectangle that contains the circle.
    '''
    if radius <0:
        return None
    else:
        new_x = x-radius
        new_y = y-radius
        return (new_x, new_y)

#########
#Q2
#########

def vote_percentage(results):
    '''
    (str) ->float
    precondition: 'yes' and 'no' need to be in a lower case
    The function counts the number of "yes" and "no". With that values, it returns the percentage of 'yes' out of 'no'
    '''
    x = results.count("yes")
    y = results.count("no")
    m = x / (x+y)
    return m


#########
#Q3
#########

def vote_percentage(results):
    '''
    (str) ->float
    precondition: 'yes' and 'no' need to be in a lower case
    The function counts the number of "yes" and "no". With that values, it returns the percentage of 'yes' out of 'no'
    '''
    x = results.count("yes")
    y = results.count("no")
    m = x / (x+y)
    return m

def vote():
    '''
    (none)-> none
    The function calls the vote_percentage function. With the percentage, it prints whether the proposal passed unanimously, with super majority, with simple majority, and the proposal fails.
    '''
    results = input("Enter the yes, no, abstained votes one by one and then press enter:\n ")
    per_of_vote = vote_percentage(results)
    if per_of_vote == 1:
        print("proposal passes unanimously")

    elif per_of_vote >= 0.6:
        print("proposal passes with super majority")

    elif per_of_vote >= 0.5:
        print("proposal passes with simple majority")

    else:
        print("proposal fails")

