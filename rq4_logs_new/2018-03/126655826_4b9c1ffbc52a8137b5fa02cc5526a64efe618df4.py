class Book:


    def __init__(self):
        self.bookID = None
        self.isbn13 = None
        self.isbn10 = None
        self.title = None
        self.copyRightDate = None
        self.type = None
        self.edition = None
        self.numberOfPages = None
        self.size = None
        self.weight = None
        self.image = None
        self.genre = None
        self.authorID = None
        self.publisherID = None

    def get_bookID(self):
        return self.bookID

    def set_bookID(self,bookID):
        self.bookID=bookID

    def get_isbn13(self):
        return self.isbn13

    def set_isbn13(self,isbn13):
        self.isbn13=isbn13

    def get_isbn10(self):
        return self.isbn10

    def set_isbn10(self,isbn10):
        self.isbn10 = isbn10

    def get_title(self):
        return self.title

    def set_title(self,title):
        self.title = title

    def get_copyRightDate(self):
        return self.copyRightDate

    def set_copyRightDate(self,copyRightDate):
        self.copyRightDate = copyRightDate

    def get_type(self):
        return self.type

    def set_type(self,type):
        self.type = type

    def get_edition(self):
        return self.edition

    def set_edition(self,edition):
        self.edition = edition

    def get_numberOfPages(self):
        return self.numberOfPages

    def set_numberOfPages(self,numberOfPages):
        self.numberOfPages = numberOfPages

    def get_size(self):
        return self.size

    def set_size(self,size):
        self.size = size

    def get_weight(self):
        return self.weight

    def set_weight(self,weight):
        self.weight = weight

    def get_image(self):
        return self.image

    def set_image(self,image):
        self.image = image

    def get_genre(self):
        return self.genre

    def set_genre(self,genre):
        self.genre = genre

    def get_authorID(self):
        return self.authorID

    def set_authorID(self,authorID):
        self.authorID = authorID

    def get_publisherID(self):
        return self.publisherID

    def set_publisherID(self,publisherID):
        self.publisherID = publisherID