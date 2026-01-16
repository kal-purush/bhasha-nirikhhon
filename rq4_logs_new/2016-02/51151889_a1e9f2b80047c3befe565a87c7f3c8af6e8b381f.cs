namespace BoardGames.Controllers
{
    using Models;
    using System.Net;
    using System.Net.Mail;
    using System.Threading.Tasks;
    using System.Web.Mvc;

    public class ContactsController : Controller
    {
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> SendMessage(MessageViewModel model)
        {
            if (ModelState.IsValid)
            {
                var body = "<p>Email From: {0} ({1})</p><p>Message:</p><p>{2}</p>";
                var message = new MailMessage();
                message.To.Add(new MailAddress("novkirishki94@gmail.com"));
                message.Subject = "Board Games Kingdom";
                message.Body = string.Format(body, model.Name, model.Email, model.Message);
                message.IsBodyHtml = true;

                using (var smtp = new SmtpClient())
                {
                    await smtp.SendMailAsync(message);
                    return Redirect(Request.UrlReferrer.ToString());
                }
            }

            return Redirect(Request.UrlReferrer.ToString());
        }
    }
}