import sys
import subprocess

def main():
    subprocess.run([sys.executable, "new_server.py"])    


if __name__ == "__main__":
    while True:
        main()
        print("\n\n---------- Server crashed ----------")
        ("print---------- Restaring server ----------")