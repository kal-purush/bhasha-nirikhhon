using HtmlAgilityPack;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace YahooScrapper
{

    class Program
    {
        public static string getBetween(string strSource, string strStart, string strEnd)
        {
            int Start, End;
            if (strSource.Contains(strStart) && strSource.Contains(strEnd))
            {
                Start = strSource.IndexOf(strStart, 0) + strStart.Length;
                End = strSource.IndexOf(strEnd, Start);
                return strSource.Substring(Start, End - Start);
            }
            else
            {
                return "";
            }
        }

        public static void save(String simbol, String fullCompanyName, String fullHireEmployees, String foundedYearString,String CityState, String MarketCap, String OpenPrice, String dateDatabase, String ClosePrice)
        {
            SqlConnection con = new SqlConnection(ConfigurationManager.ConnectionStrings["DBCS"].ConnectionString);
            con.Open();

            SqlCommand cmd = new SqlCommand("INSERT INTO Tickers(Symbols,FullCompanyName,HireEmployees,FoundedYear,CityState,MarketCap,[Date],OpenPrice,PreviousClosePrice) VALUES (@simbol,@name,@employees,@year,@city,@cap,@date,@price,@cPrice)", con);
            cmd.Parameters.AddWithValue("@simbol", SqlDbType.NVarChar).Value = simbol;
            cmd.Parameters.AddWithValue("@name", SqlDbType.NVarChar).Value = fullCompanyName;
            cmd.Parameters.AddWithValue("@employees", SqlDbType.NVarChar).Value = fullHireEmployees;
            cmd.Parameters.AddWithValue("@year", SqlDbType.NVarChar).Value = foundedYearString;
            cmd.Parameters.AddWithValue("@city", SqlDbType.NVarChar).Value = CityState;
            cmd.Parameters.AddWithValue("@cap", SqlDbType.NVarChar).Value = MarketCap;
            cmd.Parameters.AddWithValue("@date", SqlDbType.NVarChar).Value = dateDatabase;
            cmd.Parameters.AddWithValue("@price", SqlDbType.NVarChar).Value = OpenPrice;
            cmd.Parameters.AddWithValue("@cPrice", SqlDbType.NVarChar).Value = ClosePrice;

            cmd.ExecuteNonQuery();
            con.Close();
        }

        static void Main(string[] args)
        {
            string breakInput = "5";
            while (breakInput != "0")
            {
                Console.WriteLine("\nWelcome !\nIf You want to see the current trending tickers, \nor enter custom ticker symbol to see its data, please enter 1.\nIf You want to exit, enter 0!\n");
                breakInput = Console.ReadLine();
                if (breakInput == "0") {; return; }
                else
                {
                    HtmlAgilityPack.HtmlWeb web = new HtmlAgilityPack.HtmlWeb();
                    HtmlAgilityPack.HtmlDocument startPage = web.Load("https://finance.yahoo.com/trending-tickers"); // trending tikeri

                    var symbolsTable = startPage.DocumentNode.SelectSingleNode("//table"); 
                    var symbol = startPage.DocumentNode.SelectNodes("//td[@class = 'data-col0 Ta(start) Pstart(6px) Pend(15px)']/data-symbol");
                    Console.Write("Trending tickers : ");

                    foreach (var cell in symbolsTable.SelectNodes("//td[@class = 'data-col0 Ta(start) Pstart(6px) Pend(15px)']/a"))
                    {
                        string Symbols = cell.InnerText;                        
                        Console.Write( Symbols + ",");
                    }

                    Console.WriteLine("\n\nEnter ticker symbol : ");
                    string simbol = "";
                    simbol = Console.ReadLine();
                    var url = "https://finance.yahoo.com/quote/" + simbol + "/profile?p=" + simbol; //profil odabranog tikera
                    var url2 = "https://finance.yahoo.com/quote/" + simbol + "/history?p=" + simbol; //profil prethodnih podatka za određeni datum
                    var url3 = "https://finance.yahoo.com/quote/" + simbol + "?p=" + simbol; // summary određenog tikera
                    HtmlAgilityPack.HtmlDocument document3 = web.Load("" + url3 + "");
                    HtmlAgilityPack.HtmlDocument document = web.Load("" + url + "");
                    HtmlAgilityPack.HtmlDocument document2 = web.Load("" + url2 + "");

                    string foundedYearString;
                    var foundedYear = 0;
                    try
                    {

                        foundedYear = int.Parse(Regex.Match(document.DocumentNode.SelectSingleNode("//p[@class = 'Mt(15px) Lh(1.6)']").InnerText, @"\b(19|20)\d{2}\b").Value);
                        foundedYearString = foundedYear.ToString();
                    }
                    catch (Exception)
                    {
                        foundedYear = 0;
                        foundedYearString = "";
                    }

                    String fullCompanyName = document.DocumentNode.SelectSingleNode("//h1[@class='D(ib) Fz(18px)']").InnerText;

                    var fullHireEmployees = "";
                    try
                    {
                        fullHireEmployees = document.DocumentNode.SelectSingleNode("//span[@class='Fw(600)']//span").InnerText;
                    }
                    catch (Exception)
                    {
                        foundedYear = 0;

                    }
                    String CityState = "";
                    try
                    {
                        CityState = document.DocumentNode.SelectSingleNode("//p[@class='Mt(15px) Lh(1.6)']").InnerText;
                    }
                    catch(Exception) {
                        Console.WriteLine("There is no entered symbol !");
                    }
                    String headquarteredIn = Program.getBetween(CityState, "headquartered in", ".");
                    var MarketCap = document3.DocumentNode.SelectSingleNode("//span[@class = 'Trsdu(0.3s) ']").InnerText;

                    Console.WriteLine("\nFull company name is " + fullCompanyName);
                    Console.Write("It has " + fullHireEmployees + " employees.");
                    Console.WriteLine("The company was founded in " + foundedYear);
                    Console.WriteLine("It's headquartered in " + headquarteredIn);
                    Console.WriteLine("Market cap for this company is " + MarketCap);

                    Console.WriteLine("\nIf you are interested in its historical data please enter a date in format dd/mm/yyyy : ");


                    DateTime myDate = DateTime.Parse(Console.ReadLine());
                    String date = String.Format("{0:MMM dd, yyyy}", myDate);
                    date = date.Substring(1, date.Length - 2);

                    String OpenPrice = "";
                    String ClosePrice = "";
                    HtmlNode table = document2.DocumentNode.SelectSingleNode("//table");
                    bool continueProgram = true;
                    string dateDatabase = "";
                    while (continueProgram)
                    {
                        foreach (var cell in table.SelectNodes(".//tr/td"))
                        {
                            var someVariable = cell.InnerText;

                            if (someVariable.Contains(date))
                            {

                                ClosePrice = cell.NextSibling.NextSibling.NextSibling.NextSibling.InnerText;
                                Console.WriteLine("\nClose Price: " + ClosePrice);
                                Console.WriteLine("For Date: " + someVariable);
                                OpenPrice = cell.NextSibling.InnerText;
                                Console.WriteLine("Open price for this date is " + OpenPrice + ".");
                                dateDatabase = someVariable.ToString();
                                continueProgram = false;
                            }


                        }
                        if (continueProgram)
                        {
                            Console.WriteLine("\n\nWe have no data informations for selected date. If you are interested in some other date enter 1, if not, enter 2. \n");
                            breakInput = Console.ReadLine();
                            if (breakInput == "2")
                            {

                                continueProgram = false;
                                break;

                            }
                            else
                            {
                                Console.WriteLine("\nEnter date:");
                                DateTime myDates = DateTime.Parse(Console.ReadLine());

                                date = String.Format("{0:MMM dd, yyyy}", myDates);
                                date = date.Substring(1, date.Length - 2);

                            }
                        }
                        else if (continueProgram == false) { break; }
                    }


                    if (breakInput == "2") { continue; }
                    Console.WriteLine("\n\nPress 1 to save this ticker to database\nPress 0 to exit...");
                    breakInput = Console.ReadLine();
                    Program.save(simbol, fullCompanyName, fullHireEmployees, foundedYearString, headquarteredIn, MarketCap, OpenPrice, dateDatabase, ClosePrice);

                    Console.WriteLine("\n\n\nYour data successfully Saved!");
                    Console.ReadLine();
                }
            }
        }
    }
}