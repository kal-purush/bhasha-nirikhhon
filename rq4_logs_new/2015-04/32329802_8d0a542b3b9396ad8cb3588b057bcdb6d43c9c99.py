import shelve
import sys

def index(fname):
	d_user = shelve.open("usershelf")
	d_book = shelve.open("bookshelf")

	#d_user = {}
	#d_book = {}

	user_count = 0
	book_count = 0
	with open(fname, "r") as f:
		i = 0
		for line in f:
			u, v, w = line.strip().split()
			if u not in d_user:
				d_user[u] = user_count
				user_count += 1
			if v not in d_book:
				d_book[v] = book_count
				book_count += 1
			if i % 100000 == 0:
				print i
			i += 1
	d_user.close()
	d_book.close()

	print user_count, book_count

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Usage: %s <user_files>" % sys.argv[0]
	index(sys.argv[1])