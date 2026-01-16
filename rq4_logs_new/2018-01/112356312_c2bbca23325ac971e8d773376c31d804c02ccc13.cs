using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using DataDomain;
using DataPersistence;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Presentation.Models;
using Utils;

namespace Presentation.Controllers
{
    public class PersonsController : Controller
    {
        private readonly DataBaseContext _context;

        public PersonsController(DataBaseContext context)
        {
            _context = context;
        }

        // GET: People
        public async Task<IActionResult> Index()
        {
            return View(await _context.Persons.ToListAsync());
        }

        // GET: People/Details/5
        public async Task<IActionResult> Details(Guid? id)
        {
            if (!_context.Persons.Any(t => t.Id == id))
            {
                return NotFound();
            }

            var person = await _context.Persons.SingleOrDefaultAsync(m => m.Id == id);

            return View(person);
        }

        // GET: People/Create
        public IActionResult Register()
        {
            return View();
        }

        // POST: People/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Register(RegisterViewModel registerVm)
        {
            if (!ModelState.IsValid)
            {
                ModelState.AddModelError("Error", "Complete all fields.");
                return View(registerVm);
            }
            var exist = await _context.Persons
                .SingleOrDefaultAsync(m => m.EmailAddress == registerVm.Email);
            if (exist != null)
            {
                ModelState.AddModelError("Error", "Email address is used.");
                return View(registerVm);
            }
            exist = await _context.Persons
               .SingleOrDefaultAsync(m => m.Username == registerVm.UserName);
            if (exist != null)
            {
                ModelState.AddModelError("Error", "Username is used.");
                return View(registerVm);
            }
            exist = await _context.Persons
                .SingleOrDefaultAsync(m => m.Cnp == registerVm.Cnp);
            if (exist != null)
            {
                ModelState.AddModelError("Error", "Cnp is used.");
                return View(registerVm);
            }

            var user = new Person
            {
                Id = new Guid(),
                Cnp = registerVm.Cnp,
                FirstName = registerVm.FirstName,
                LastName = registerVm.LastName,
                Username = registerVm.UserName,
                EmailAddress = registerVm.Email,
                Password = CryptingUtils.Encode(registerVm.Password),
                Gender = registerVm.Gender,
                Birthday = registerVm.Birthday,
                Role = "Patient"
            };
            _context.Add(user);
            await _context.SaveChangesAsync();
            return RedirectToAction("Login", "Persons");

        }

        // GET: People/Create
        public IActionResult Login()
        {
            return View();
        }

        // POST: People/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Login(LoginViewModel loginVm)
        {
            if (!ModelState.IsValid)
            {
                ModelState.AddModelError("Error", "Complete all fields.");
                return View();
            }

            var isOk = await _context.Persons
                    .SingleOrDefaultAsync(m => m.Username == loginVm.UserName && m.Password == CryptingUtils.Encode(loginVm.Password));
            if (isOk == null)
            {
                ModelState.AddModelError("Error", "User or password is wrong!");
                return View();
            }

            HttpContext.Session.SetString("user_name", loginVm.UserName);
            HttpContext.Session.SetString("role", isOk.Role);
            HttpContext.Session.SetString("email", isOk.EmailAddress);
            return RedirectToAction("Index", isOk.Role == "Patient" ? "Patients" : "Medics");
        }

        public IActionResult Logout()
        {
            HttpContext.Session.SetString("user_name", "");
            HttpContext.Session.SetString("rol", "");
            HttpContext.Session.SetString("email", "");
            return RedirectToAction("Login", "Persons");
        }


        // GET: People/Edit/5
        public async Task<IActionResult> Edit(Guid? id)
        {
            if (!_context.Persons.Any(t => t.Id == id))
            {
                return NotFound();
            }

            var person = await _context.Persons.SingleOrDefaultAsync(m => m.Id == id);
            return View(person);
        }

        // POST: People/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(Guid id, [Bind("Id,Cnp,FirstName,LastName,EmailAddres,Username,Password,Gender,Birthday,Role")] Person person)
        {
            if (!_context.Persons.Any(t => t.Id == id))
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                _context.Update(person);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(person);
        }

        // GET: People/Delete/5
        public async Task<IActionResult> Delete(Guid? id)
        {
            if (!_context.Persons.Any(t => t.Id == id))
            {
                return NotFound();
            }

            var person = await _context.Persons
                .SingleOrDefaultAsync(m => m.Id == id);

            return View(person);
        }

        // POST: People/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(Guid id)
        {
            var person = await _context.Persons.SingleOrDefaultAsync(m => m.Id == id);
            _context.Persons.Remove(person);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        [HttpPost, ActionName("Error")]
        [ValidateAntiForgeryToken]
        public IActionResult Error()
        {
            return View("Error", new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}