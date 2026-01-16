import sqlite3 as lite



#functionality goes here

class DatabaseManage(object):
	def __init__(self):
		global con
		try:
			con = lite.connect('courses.db')
			with con:
				cur = con.cursor()
				cur.execute("CREATE TABLE IF NOT EXISTS course(Id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, DESCRIPTION TEXT, PRICE TEXT, is_private BOOLEAN NOT NULL DEFAULT 1)")
		except Exception:
			print("unable to create DB")

	#TODO: create data
	def insert_data(self, data):
		try:
			with con:
				cur = con.cursor()
				cur.execute("INSERT INTO course(name,description,price,is_private) VALUES (?,?,?,?)", data)
				return True
		except Exception:
			return False

	#TODO: read data
	def fetch_data(self):
		try:
			with con:
				cur = con.cursor()
				cur.execute("SELECT * FROM course")
				return cur.fetchall()

		except Exception:
			return False
	#TODO: delete data
	def delete_data(self, id):
		try:
			with con:
				cur = con.cursor()
				sql = "DELETE FROM course WHERE id = ?"
				cur.execute(sql, [id])
				return True
		except Exception:
			return False
		


#Todo: Provide interface to user

def main():
	print("*"*40)
	print("\n:: COURSE MANAGEMENT : : \n")
	print("*"*40)
	print("\n")

	db = DatabaseManage()

	print("#"*40)
	print("\n:: USER MANUAL : : \n")
	print("#"*40)

	print("\nPress 1. Insert a  new course\n")
	print("Press 2. show all courses\n")
	print("Press 3. delete a course(Need id of course)\n")
	print('#'*40)
	print("\n")

	choice = input("\n Enter a choice: ")

	if choice == "1":
		name = input("\n Enter course name: ")
		description = input("\n Enter course description: ")
		price = input("\n Enter course price: ")
		private = input("\n In this course private(0/1): ")


		if db.insert_data([name, description, price, private]):
			print("Course was inserted")

		else:
			print("OOPS something is wrong")

	elif choice == "2":
		print("\n:: Course List ::")

		for index, item in enumerate(db.fetch_data()):
			#print("\n sl no: " + str(index + 1))
			print("Course ID: " + str(item[0]))
			print("Course Name: " + str(item[1]))
			print("Course description: " + str(item[2]))
			print("Course Price: " + str(item[3]))
			private = 'Yes' if item[4] else 'NO'
			print("Is Private :" + private)
			print("\n")

	elif choice == "3":
		record_id = input("Enter the course ID: ")

		if db.delete_data(record_id):
			print("Course was deleted")
		else:
			print("something went wrong")
	else:
		print("\n BAD CHOICE")



if __name__ == "__main__":
	main()






















