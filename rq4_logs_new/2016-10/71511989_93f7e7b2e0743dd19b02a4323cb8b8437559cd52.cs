using MySql.Data.MySqlClient;
using Rechnungsversand;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    public class Invoice
    {
        public static List<Invoice> GetInvoices { get { return Directory.GetFiles(@"W:\\Rechnungsversand\\Rechnungen", "*.pdf").Select(path => Path.GetFileName(path)).Select(o => new Invoice(o)).ToList(); } }

        public string Name { get { return filename.Replace(".pdf", ""); } }

        private string filename;

        public Invoice(string f)
        {
            filename = f;
        }

        public async Task Send()
        {
            try
            {
                MySqlCommand command = dbconnect.connection.CreateCommand(); // When you use this to create a command you don't have to initialize it in the next line with 'new' keyword, because that makes this particular line pointless.
                command.CommandText = string.Format("SELECT `SheetNr`,`AuftragsNr`,`AuftragsKennung`,`VorgangNr`,`Anschrift_Email` FROM `fk_auftrag` WHERE `AuftragsNr` = '{0}' AND `AuftragsKennung` = 3;", filename.Replace(".pdf", ""));
                var reader = await command.ExecuteReaderAsync();
                string adresse = reader.GetString(4);
                string betreff = "Ihre Rechnung mit der Rechnungsnr: " + Name + " von Wunschreich";
                string rnr = reader.GetString(1);
                await send_mail.send(adresse, betreff, filename, rnr);
            }
            catch (Exception ex) { Console.WriteLine(ex.Message); }
        }
    }
}