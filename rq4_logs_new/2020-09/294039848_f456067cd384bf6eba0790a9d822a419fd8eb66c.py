sortThis = [1, 9, 3, 5, 2, 7, 6, 4, 8]
print("This will sort the default list.\nEdit the code to select another list.\n")

def BubbleSort():
		
	n = 0
	while n < len(sortThis) - 1:
		i = 0
		
		while i < len(sortThis) - n - 1:
			j = i + 1

			a = sortThis[i]
			b = sortThis[j]

			if a > b:
				sortThis[i] = b
				sortThis[j] = a
				print("swapped")
				

				#swap(a, b)
			else:
				print(str(i) + " " + str(j) + " " + "nothing was swapped")

			i += 1
		n += 1
	print(sortThis)
		


BubbleSort()