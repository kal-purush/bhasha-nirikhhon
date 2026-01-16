def main():
    hex_colours = {"aliceblue": "#f0f8ff", "antiquewhite": "#faebd7", "aquamarine": "#7fffd4", "azure": "#f0ffff",
               "beige": "#f5f5dc", "black": "#000000", "blue": "#0000ff", "brown": "#a52a2a", "burlywood": "#deb887", "cadetblue": "#5f9ea0"}
    query = input("Look up a colour's hexadecimal value: ").lower()
    while query != hex_colours:
        print("Could not find this colour.")
        query = input("Look up a colour's hexadecimal value: ").lower()
    print(hex_colours[query])

main()