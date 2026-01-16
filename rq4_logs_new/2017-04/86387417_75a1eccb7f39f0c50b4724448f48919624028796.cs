using System;
namespace ProlificLibrary
{
    public static class Constants
    {
        public static class ErrorMessages 
        {
            public static string RequiredFields = "Title and author fields are required";
        }

        public struct SuccessMessages 
        {
            public static string Success = "Success!";
            public static string BookAddedWithSuccess = "Book was added to list.";
            public static string BookUpdatedWithSuccess = "Your changes were submitted.";
        }
    }
}