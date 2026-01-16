namespace BookAPI.Constants
{
    public static class Message
    {
        public const string NOT_FOUND = "Error... NotFound";
        public const string DELETION = "Deleted Successfully";
        public const string Updation = "Updated Successfully";

        public const string INVALID_CRED = "Invalid Credentials";
        public const string NO_USER = "User does not exist";
        public const string NO_BOOK = "Error, Book Not Found!";
        public const string NO_BOOK_CONTEXT = "Entity set 'BookDbContext.Books'  is null.";
        public const string NO_AUTHOR = "No Author Found";
        public const string NO_AUTHOR_MODEL = "Entity set 'BookDbContext.AuthorModel'  is null.";
    }
}