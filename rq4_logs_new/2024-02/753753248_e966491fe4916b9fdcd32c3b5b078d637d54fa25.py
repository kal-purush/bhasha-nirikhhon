import matplotlib.pyplot as plt
print(plt.style.available)

x_values = range(1, 1001)
y_values = [x ** 2 for x in x_values]

input_values = [1, 2, 3, 4, 5]
squares = [1, 4, 9, 16, 25]

plt.style.use('grayscale')
fig, ax = plt.subplots()
# ax.scatter(x_values, y_values, c=(0, 0.8, 0), s=10)
ax.scatter(x_values, y_values, c=y_values, cmap=plt.cm.Blues, s=10)
ax.plot(input_values, squares, linewidth=3)

# chart title and label axes
ax.set_title('Square Numbers', fontsize=24)
ax.set_xlabel('Value', fontsize=14)
ax.set_ylabel('Square of value', fontsize=14)

# range of each axis
ax.axis =([0, 1100, 0, 1100000])

# size of tick labels
ax.tick_params(axis='both', which='major', labelsize=14)
plt.show()

# save automatically
plt.savefig('square_plot.png', bbox_inches='tight')