using LibraryWebApp.ViewModels;

namespace LibraryWebApp.Helpers
{
    public class ErrorViewModelTypes
    {
        public static ErrorViewModel NotFound(string itemNotFound)
        {
            return new ErrorViewModel { StatusCode = "404", Description = $"Oops! The {itemNotFound} you're looking for can't be found." };
        }

        public static ErrorViewModel AccessDenied()
        {
            return new ErrorViewModel { StatusCode = "403", Description = "Uh-oh! You don’t have permission to view this page or resource." };
        }

        public static ErrorViewModel NoAvailableBooks()
        {
            return new ErrorViewModel { StatusCode = "404", Description = "Unfortunately there aren't any available copies of this book." };
        }

        public static ErrorViewModel OverdueBooks()
        {
            return new ErrorViewModel { StatusCode = "403", Description = "You must return your overdue books before renting new ones." };
        }
    }
}