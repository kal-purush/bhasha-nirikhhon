using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using KWFCI.Models;
using KWFCI.Repositories;
using Microsoft.AspNetCore.Identity;
using MimeKit;
using MailKit.Net.Smtp;

namespace KWFCI.Controllers
{
    [Authorize(Roles = "Staff")]
    public class SettingsController : Controller
    {
        private IStaffProfileRepository profileRepo;
        private UserManager<StaffUser> userManager;

        public SettingsController(IStaffProfileRepository repo, UserManager<StaffUser>usrMgr)
        {
            profileRepo = repo;
            userManager = usrMgr;
        }


        public IActionResult Index()
        {
            if (Helper.StaffProfileLoggedIn == null)
                return RedirectToAction("Login", "Auth");
            // TODO: Grab logged in user profile
            var me = Helper.DetermineProfile(profileRepo);
            // TODO: Send Staff Profile to View
            return View(me);
        }

        [HttpPost]
        public IActionResult Index(StaffProfile p)
        {
            if (p != null)
            {
                StaffProfile profile = profileRepo.GetStaffProfileByID(p.StaffProfileID);
                profile.Email = p.Email;
                profile.User.UserName = p.Email;
                profile.User.NormalizedUserName = p.Email.ToUpper();
                profile.FirstName = p.FirstName;
                profile.LastName = p.LastName;
                profile.EmailNotifications = p.EmailNotifications;

                int verify = profileRepo.UpdateStaff(profile);
                if (verify == 1)
                {
                    //TODO add feedback of success
                    return RedirectToAction("Index","Home");
                }
                else
                {
                    //TODO add feedback for error
                }
            }
            else
            {
                ModelState.AddModelError("", "User Not Found");
            }
            return View(p);
        }

        [Route("Settings/Send")]
        public IActionResult SendPasswordResetLink(int id)
        {
            StaffProfile profile = profileRepo.GetStaffProfileByID(id);
            StaffUser user = userManager.
                 FindByNameAsync(profile.User.UserName).Result;

            if (user == null)
            {
                return View("Index");
            }

            var token = userManager.
                  GeneratePasswordResetTokenAsync(user).Result;

            var resetLink = Url.Action("ResetPassword",
                            "Auth", new { token = token },
                             protocol: HttpContext.Request.Scheme);

            var email = new MimeMessage();
            email.From.Add(new MailboxAddress("KWFCI", "do-not-reply@kw.com"));
            email.Subject = "Password Reset";
            email.Body = new TextPart("plain")
            {
                Text = "Click the link to reset your password " + resetLink
            };
            email.To.Add(new MailboxAddress(user.UserName));

            using (var client = new SmtpClient())
            {
                client.ServerCertificateValidationCallback = (s, c, h, e) => true;

                client.Connect("smtp.gmail.com", 587, false);

                client.AuthenticationMechanisms.Remove("XOAUTH2");
                client.Authenticate("kwfamilycheckin", "Fancy123!");

                client.Send(email);
                client.Disconnect(true);
            }

            //TODO: Pop up Alert box saying the reset link has been sent to their email
            return View("Index",profile);
        }

    }
}