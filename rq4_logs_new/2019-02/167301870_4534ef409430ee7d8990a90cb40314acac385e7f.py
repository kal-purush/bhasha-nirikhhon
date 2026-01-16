import os, numpy

class FileManipulation(object):

    def __init__(self, path):
        self.path = path
        self.content = list()
    
    def get_file_contents(self):
        return self.content
    
    def read_file(self):
        with open(self.path, 'r') as f:
            self.content.extend(f.read().split("\n"))
    
def main():
    # a test driver 
    path = "sample.txt"
    fm = FileManipulation(path)
    fm.read_file(); print(fm.get_file_contents())

if __name__ == "__main__":
    main()