using LibraryWebApp.Helpers;
using LibraryWebApp.Models;
using LibraryWebApp.ViewModels;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using NuGet.Protocol.Plugins;
using Sprache;
using static LibraryWebApp.Controllers.HomeController;

namespace LibraryWebApp.Controllers
{
    [Authorize(Roles = Globals.Roles.Admin)]
    public class AdminController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly IBookCoverImageManager _bookCoverImageManager;

        public AdminController(ApplicationDbContext con, IBookCoverImageManager img)
        {
            _context = con;
            _bookCoverImageManager = img;
        }

        public async Task<IActionResult> Panel(AdminPanelViewModel? vm)
        {
            return View(vm);
        }

        public async Task<IActionResult> GetTopBooks()
        {
            IQueryable<Book> books = _context.Books;
            books = books.Include(b => b.RentedBooks).OrderByDescending(b => b.RentedBooks.Count()).Take(10);
            var bookData = books.Select(b => new BookSimplified
            {
                Id = b.Id,
                Title = b.Title,
                CoverImageUrl = b.CoverImageUrl != null ? b.CoverImageUrl : _bookCoverImageManager.DefaultNoBookCoverImagePath
            }).ToList();

            var viewModel = new AdminPanelViewModel
            {
                TopBooks = bookData
            };
            return View("Panel", viewModel);

        }
        public async Task<IActionResult> GetTopUsers()
        {
            IQueryable<ApplicationUser> users = _context.Users;
            users = users.Include(b => b.RentedBooks).OrderByDescending(b => b.RentedBooks.Count()).Take(10);
            var viewModel = new AdminPanelViewModel
            {
                TopUsers = users.ToList()
            };
             return View("Panel", viewModel);
        }

        public async Task<IActionResult> GetAllUsers()
        {
            IQueryable<ApplicationUser> users = _context.Users;
            var actives = GetActiveUsers();
            var inactives = GetInactiveUsers();

            var viewModel = new AdminPanelViewModel
            {
                InactiveUsers = inactives.ToList(),
                ActiveUsers = actives.ToList(),
            };

            return View("Panel", viewModel);
        }

        public IQueryable<ApplicationUser> GetActiveUsers()
        {
            var activeUsers = _context.Users
            .Where(user => _context.RentedBooks
            .Where(rentedBook => rentedBook.UserId == user.Id && rentedBook.ReturnedAt == null)
            .Any()) 
            .Distinct(); 
            return activeUsers;
        }

        public IQueryable<ApplicationUser> GetInactiveUsers()
        {
            var inactiveUsers = _context.Users
              .Where(user => !_context.RentedBooks
              .Any(rentedBook => rentedBook.UserId == user.Id && rentedBook.ReturnedAt == null))
              .Distinct();
            return inactiveUsers;
        }

        public async Task<IActionResult> Details(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user == null)
            {
                // TODO better message
                return NotFound();
            }

            return View(user);
        }

        public async Task<IActionResult> Delete(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user == null)
            {
                return NotFound();
            }

            return View(user);
        }

        [HttpPost, ActionName("Delete")]
        public async Task<IActionResult> DeletePOST(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user == null)
            {
                // TODO better message
                return NotFound();
            }

            _context.Users.Remove(user);
            await _context.SaveChangesAsync();

            return RedirectToAction(nameof(Panel));
        }

        public async Task<IActionResult> Edit(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user == null)
            {
                // TODO better message
                return NotFound();
            }

            var vm = new EditAppUserVM
            {
                Id = user.Id,
                UserName = user.UserName,
                Email = user.Email,
                PhoneNumber = user.PhoneNumber
            };

            return View(vm);
        }
        
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(EditAppUserVM vm)
        {
            var user = await _context.Users.FindAsync(vm.Id);

            if (user == null) 
            {
                // TODO better error message - user not found view
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                user.UserName = vm.UserName; 
                user.NormalizedUserName = vm.UserName.ToUpper();
                user.Email = vm.Email;
                user.NormalizedEmail = vm.Email.ToUpper();
                user.PhoneNumber = vm.PhoneNumber;

                await _context.SaveChangesAsync();

                return RedirectToAction(nameof(Panel));
            }

            return View(vm);
        }

        public async Task<IActionResult> ReturnAll(string id) 
        {
            var rentedBooks = await _context.RentedBooks
                .Where(b => b.UserId == id && b.ReturnedAt == null)
                .ToListAsync();

            var dateNow = DateOnly.FromDateTime(DateTime.Now);

            foreach (var rentedBook in rentedBooks)
            {
                var book = await _context.Books.FindAsync(rentedBook.BookId);
                book.AvailableCount += 1;
                rentedBook.ReturnedAt = dateNow;
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Panel));
        }

        public async Task<IActionResult> ReturnLate(string id)
        {
            var dateNow = DateOnly.FromDateTime(DateTime.Now);

            // Get all rented books of a user with this id, which are not yet returned AND the deadline is in the past meaning they are late to returning them
            var rentedBooks = await _context.RentedBooks
                .Where(b => b.UserId == id && b.ReturnedAt == null && b.Deadline < dateNow)
                .ToListAsync();

            foreach (var rentedBook in rentedBooks)
            {
                var book = await _context.Books.FindAsync(rentedBook.BookId);
                book.AvailableCount += 1;
                rentedBook.ReturnedAt = dateNow;
            }

            await _context.SaveChangesAsync(); 
            return RedirectToAction(nameof(Panel));
        }

        public async Task<IActionResult> BanUser(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user != null && user.IsBanned == false)
            {
                user.IsBanned = true;
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Panel));
            }

            // TODO need an error message that no such user exists in order to ban him
            return NotFound();
        }

        public async Task<IActionResult> UnbanUser(string id)
        {
            var user = await _context.Users.FindAsync(id);

            if (user != null && user.IsBanned) 
            {
                user.IsBanned = false;
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Panel));
            }

            // TODO need an error message that no such banned user exists in order to unban him
            return NotFound();
        }
    }
}