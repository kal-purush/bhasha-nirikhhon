fruit_to_color={
    'banana':'Yellow',
    'Cheery':'Red',
    'orange':'Orange',
    'pear':'green',
    'peach':'orange',
    'plum':'purple',
    'pomogranate':'red',
    'strawberry':'red'
    }

color_to_fruit={}

for fruit in fruit_to_color:
    color=fruit_to_color[fruit]

    #If colour is not already a key in the accumulator,
    #add colour:[fruit] as entry
    

    if not(color in color_to_fruit):
        color_to_fruit[color]=[fruit]

    #Otherwise, append fruit to the existing list
    else:
        color_to_fruit[color].append(fruit)
        