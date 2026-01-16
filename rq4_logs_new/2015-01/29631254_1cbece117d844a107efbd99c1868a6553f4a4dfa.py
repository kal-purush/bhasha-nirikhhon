"""Recursive implementation of a binary search"""

def bsearch(l, n):
    """takes a sorted list l that contains a tuple of key/value
       pairs, returns value of n"""
    if l[int(len(l)/2)][0] == n:
        return l[int(len(l)/2)][1]
    elif len(l) == 1: # not in the list
        return None
    elif l[int(len(l)/2)][0] > n:
        return bsearch(l[:(int(len(l)/2))], n)
    elif l[int(len(l)/2)][0] < n:
        return bsearch(l[(int(len(l)/2)):], n)


if __name__ == "__main__":
    """example usage"""
    text = """add|Add file contents to the index
bisect|Find by binary search the change that introduced a bug
branch|List, create, or delete branches
checkout|Checkout a branch or paths to the working tree
clone|Clone a repository into a new directory
commit|Record changes to the repository
diff|Show changes between commits, commit and working tree, etc
fetch|Download objects and refs from another repository
grep|Print lines matching a pattern
init|Create an empty Git repository or reinitialize an existing one
log|Show commit logs
merge|Join two or more development histories together
mv|Move or rename a file, a directory, or a symlink
pull|Fetch from and integrate with another repository or a local branch
push|Update remote refs along with associated objects
rebase|Forward-port local commits to the updated upstream head
reset|Reset current HEAD to the specified state
rm|Remove files from the working tree and from the index
show|Show various types of objects
status|Show the working tree status
tag|Create, list, delete or verify a tag object signed with GPG"""

    git_commands = []
    for line in text.split("\n"):
        git_commands.append(line.split("|"))
    git_commands = sorted(git_commands)
    print(bsearch(git_commands, "pull"))