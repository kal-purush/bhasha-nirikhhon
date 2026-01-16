from collections import deque
linked_list = deque([1, 2, 3, 4, 5])

def reverse(linked_list):
    linked_list_reversed = deque([])
    for element in linked_list:
        linked_list_reversed.appendleft(element)
    return linked_list_reversed

print(linked_list, reverse(linked_list))