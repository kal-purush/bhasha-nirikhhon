
from math import factorial
from this import d
#on managed nodes 
echo "ansible ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/ansible

#on managed nodes
d /etc/ansible/facts.d
-> f .fact 