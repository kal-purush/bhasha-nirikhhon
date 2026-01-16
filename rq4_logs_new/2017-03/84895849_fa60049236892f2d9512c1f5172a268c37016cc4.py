import io

with io.FileIO("sched.txt", "w") as file:
    file.write("-")
	file.close()