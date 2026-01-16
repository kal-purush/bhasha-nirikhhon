using LibraryWebApp.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.Web.CodeGenerators.Mvc.Templates.Blazor;
using System.Security.Claims;

namespace LibraryWebApp.Controllers
{
    public class BookRentalsController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;
        public BookRentalsController(ApplicationDbContext con, UserManager<ApplicationUser> userManager)
        {
            _context = con;
            _userManager = userManager;
        }

        [Authorize]
        public async Task<IActionResult> Details()
        {
            var currentUserId = _userManager.GetUserId(User);

            var rentedBooks = await _context.RentedBooks
               .Include(a => a.Book)
               .Include(a => a.User)
               .Where(a => a.UserId == currentUserId)
               .ToListAsync();

            return View(rentedBooks);
        }

        [Authorize]
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Rent(int bookId)
        {
            Book? book = await _context.Books.FindAsync(bookId);
            if (book == null)
            {
                return NotFound();
            }

            if (book.AvailableCount == 0)
            {
                // TODO maybe a better custom error view - the book cannot be rented, no available copies
                // or maybe a ModelState error and redirect to the book details page?

                return NotFound();
            }
            
            var userId = User.FindFirstValue(ClaimTypes.NameIdentifier);
            bool hasOverdueBooks = _context.RentedBooks.Any(r => r.UserId == userId && r.ReturnedAt == null && r.Deadline < DateOnly.FromDateTime(DateTime.Now));

            if (hasOverdueBooks)
            {
                // TODO add an error page or redirect to proper place, due to user having unreturned books that are overdue
                return NotFound();
            }

            book.AvailableCount -= 1;

            DateOnly today = DateOnly.FromDateTime(DateTime.Now);
            DateOnly returnDate = today.AddDays(Globals.BookRentDayLimit);

            var entry = new RentedBook()
            {
                UserId = userId,
                BookId = bookId,
                RentalDate = today,
                Deadline = returnDate
            };

            _context.RentedBooks.Add(entry);
            await _context.SaveChangesAsync();

            return RedirectToAction("Index", "Home");
        }

        [Authorize]
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Return(int bookId)
        {
            var rBook = await _context.RentedBooks.FindAsync(bookId);

            if (rBook != null)
            {
                var userId = User.FindFirstValue(ClaimTypes.NameIdentifier);

                // If you are not the one who took the book, you cannot return it, unless you are an admin and you are fixing some sort of bug
                if (rBook.UserId != userId && User.IsInRole("Admin") == false)
                {
                    // TODO better error message
                    return NotFound();
                }

                var book = await _context.Books.FindAsync(rBook.BookId);

                book.AvailableCount += 1;
                _context.Books.Update(book);
                rBook.ReturnedAt = DateOnly.FromDateTime(DateTime.Now);

                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Details));
            }

            // TODO a better error message - the book to return was not found
            return NotFound();
        }
    }
}