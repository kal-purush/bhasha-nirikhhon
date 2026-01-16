from strukturdata import *

# membuat objek RBT
rbt = RedBlackTree()

# Memasukkan key dan value kedalam rbt
# key berupa string, value berupa list (nantinya mau diganti ke Linked List atau yang lain)
rbt.insert("key", "lokasi/value")
rbt.insert("a", "value")
rbt.insert("d", "value")
rbt.insert("c", "value")
rbt.insert("b", "value")


# Print Tree
rbt.print_tree()


# Melakukan pencarian
x = rbt.query("a")
print(f"Key = {x.key}, Value = {x.loc}")


# Delete key
rbt.delete("c")

rbt.print_tree()

