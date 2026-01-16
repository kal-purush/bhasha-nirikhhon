using System.Text;

namespace HotelReservation
{
    public static class CsvSerializer
    {
        public static void Save(string filePath, List<Account> accounts)
        {
            StringBuilder csv = new StringBuilder();
            csv.AppendLine("ID,Name,Email,Phone,Password");

            foreach (var account in accounts)
            {
                csv.AppendLine($"{account.ID},{account.Name},{account.Email},{account.PhoneNumber},{account.Password}");
            }

            File.WriteAllText(filePath, csv.ToString());
        }
        public static List<Account> LoadFromCsv(string filePath)
        {
            var accounts = new List<Account>();

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("File not found.", filePath);
            }

            var lines = File.ReadAllLines(filePath);

            if (lines.Length < 2)
            {
                throw new Exception("CSV file is empty or missing headers.");
            }

            for (int i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');

                if (values.Length != 5)
                {
                    throw new Exception($"Invalid data format in line {i + 1}");
                }

                var account = new Account
                {
                    ID = UID.Parse(values[0]),
                    Name = values[1],
                    Email = values[2],
                    PhoneNumber = values[3],
                    Password = values[4]
                };

                accounts.Add(account);
            }

            return accounts;
        }

    }
}