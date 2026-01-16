using HtmlAgilityPack;
using Restauracja.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Restauracja.Utilities
{
    public class EmailService
    {
        public const string EMAIL_SUBJECT = "Nowe zamówienie klienta";
        public const string GMAIL_SMTP_HOST = "smtp.gmail.com";
        public const int GMAIL_SMTP_PORT = 587;

        public Dictionary<string, string> ComposingParts { get; set; } = new Dictionary<string, string>();

        public string Sender { get; set; }
        public string Recipent { get; set; }
        public OrderPOCO Order { get; set; }


        public EmailService(string sender, string recipent, OrderPOCO order)
        {
            Sender = sender;
            Recipent = recipent;
            Order = order;
        }

        public string GenerateEmail()
        {
            var resourceName = Assembly.GetExecutingAssembly().GetManifestResourceNames().Where(q => q.Contains("ristorante_footer_pattern.html")).FirstOrDefault();
            var currency = CultureInfo.GetCultureInfo("pl-PL").NumberFormat.CurrencySymbol;
            using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    StringBuilder builder = new StringBuilder();

                    string html = reader.ReadToEnd();
                    //html.fin
                    //var doc = new HtmlWeb().Load()

                    string replacedHtml = html
                        .Replace("$$CLIENT_EMAIL$$", Sender)
                        .Replace("$$PRODUCT_ONE$$", 
                        builder
                        .Append(Order.Products[0].Name)
                        .Append(" ")
                        .Append(Order.Products[0].Price)
                        .Append(" ").Append(currency).Append(" x ")
                        .Append(Order.Products[0].Quantity)
                        .Append(" = ")
                        .Append(Order.Products[0].Price * Order.Products[0].Quantity)
                        .Append(" ")
                        .Append(currency)
                        .ToString())
                        .Replace("$$PRODUCT_TWO$$", 
                        builder.Clear().Append(Order.Products[1].Name).Append(" ").Append(Order.Products[1].Price).Append(" ").Append(currency).Append(" x ").Append(Order.Products[1].Quantity).Append(" = ").Append(Order.Products[1].Price * Order.Products[1].Quantity).Append(" ").Append(currency).ToString())
                        .Replace("$$FINAL_COST$$", builder.Clear().Append(Order.FinalCost.ToString()).Append(" ").Append(currency).ToString());

                    File.WriteAllText(@"ristorante_footer_to_sent.html", html, Encoding.UTF8); //save (cache email to be sent)
                    return html;
                }
            }
        }
    }
}