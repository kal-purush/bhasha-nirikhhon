good_widgets = int(input("How many good widgets have been produced this shift? "))
rejects_widgets = int(input("How many reject widgets have been produced this shift? "))
total_widgets = good_widgets + rejects_widgets
rejects_percent = (rejects_widgets / total_widgets) * 100
print("The percentage of rejects is", rejects_percent, "%")

if rejects_percent <= 3:
    # Percentage of rejects is less than 3%, so it is good.
    print("Shift output is good!")
elif rejects_percent <= 5:
    # Percentage of rejects is greater than 3%, but less than 5%, so it is satisfactory.
    print("Shift output is satisfactory.")
else:
    # Percentage of rejects is greater than 5%, so it is unsatisfactory.
    print("Shift output is unsatisfactory.")