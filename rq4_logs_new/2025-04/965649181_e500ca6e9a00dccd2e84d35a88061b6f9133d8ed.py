"""
LINKED LISTS (Linked Structure)
A linked list connects these nodes like this:
[Data|Next] → [Data|Next] → [Data|Next] → None

Example:

node1 = Node("a")
node2 = Node("b")
node3 = Node("c")

node1.next = node2
node2.next = node3

1. Singly Linked List

Each node points to the next node only.

[1] → [2] → [3] → None

2. Doubly Linked List

Each node has a previous and next pointer.

None ← [1] ←→ [2] ←→ [3] → None

3. Circular Linked List

The last node points back to the head.

[1] → [2] → [3] → [1]
"""

class BookNode:
    """Node for a doubly linked list"""
    def __init__(self, book_data):
        self.book = book_data
        self.next = None
        self.prev = None

class LinkedLibrary:
    """A doubly linked list implementation"""
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0
    
    def add_book(self, book_data):
        """Add an element to the end of the list"""
        new_node = BookNode(book_data)
        if not self.head:
            self.head = new_node
            self.tail = new_node
        else:
            new_node.prev = self.tail
            self.tail.next = new_node
            self.tail = new_node
        self.size += 1
    
    def delete_book(self, title):
        """Remove the first occurrence of data from the list"""
        if not self.head:
            return False
        
        current = self.head
        while current:
            if current.book["title"] == title:
                # Remove node from the list
                if current.prev:
                    current.prev.next = current.next
                else:
                    self.head = current.next
                
                if current.next:
                    current.next.prev = current.prev
                else:
                    self.tail = current.prev
                
                self.size -= 1
                return True
            current = current.next
        return False
    
    def find_book(self, title):
        current = self.head
        while current:
            if current.book["title"] == title:
                return current.book
            current = current.next
        return None

    def get_all_books(self):
        books = []
        current = self.head
        while current:
            books.append(current.book)
            current = current.next
        return books

library = LinkedLibrary()

library.add_book({"title": "1984", "author": "George Orwell"})
library.add_book({"title": "Brave New World", "author": "Aldous Huxley"})
library.add_book({"title": "Fahrenheit 451", "author": "Ray Bradbury"})

print(library.find_book("1984"))

library.delete_book("Brave New World")

library.add_book({
    "title": "Foundation",
    "author": "Isaac Asimov",
    "year": 1951,
    "genres": ["science fiction", "space opera"],
    "available": True
})

library.add_book({
    "title": "2001: A Space Odyssey",
    "author": "Arthur C. Clarke",
    "year": 1968,
    "genres": ["science fiction", "space exploration"],
    "available": True
})
print(library.get_all_books())

import json

# Export current linked list to books.json
def export_linked_books_to_json(linked_library, filename="books.json"):
    books = linked_library.get_all_books()
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=2)
    print(f"Updated {filename} with {len(books)} books.")

# Usage
export_linked_books_to_json(library)
