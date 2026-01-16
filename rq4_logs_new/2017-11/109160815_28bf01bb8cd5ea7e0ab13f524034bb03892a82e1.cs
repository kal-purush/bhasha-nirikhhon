using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ELM.Model
{
    static class FileParser
    {
        private static Dictionary<String, String> words = new Dictionary<String, String>();
        private static ArrayList messages = new ArrayList();

        private static string path = System.Environment.CurrentDirectory;

        public static Dictionary<string, string> Words { get => words; }
        public static ArrayList Messages { get => messages; }

        public static void Initialise()
        {
            using (var rd = new StreamReader(path + @"\textspeak\textwords.csv"))
            {
                while (!rd.EndOfStream)
                {
                    var splits = rd.ReadLine().Split(',');
                    string expanded = "";
                    if (splits.Length > 2)
                    {
                        expanded = splits[1] + ", " + splits[2];
                        expanded = expanded.Replace('"'.ToString(), "");
                    }
                    else
                        expanded = splits[1];

                    Words.Add(splits[0], expanded);
                }
            }            
        }

        public static void ReadFile(String fileName)
        {
            using (var rd = new StreamReader(fileName))
            {
                while (!rd.EndOfStream)
                {
                    string line = rd.ReadLine();
                    Console.WriteLine(line);

                    if(String.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }
                    if (line[0].ToString().Equals("S"))
                    {
                        string id = line;
                        string sender = rd.ReadLine();
                        if (!sender.ValidatePhoneNumber())
                            throw new Exception("Invalid phone number in Message with ID: " + id);
                        string text = rd.ReadLine();
                        text = text.ReplaceTextSpeak();
                        
                        SMSState sms = new SMSState(id, sender, text);
                        Console.WriteLine(sms.ToString());
                        Messages.Add(sms);
                    }
                    if(line[0].ToString().Equals("T"))
                    {
                        string id = line;
                        string sender = rd.ReadLine();
                        if (!sender.ValidateTwitterUser())
                            throw new Exception("Invalid twitter user name in Tweet with ID: " + id);
                        string text = rd.ReadLine();
                        text = text.ReplaceTextSpeak();

                        TweetState tweet = new TweetState(id, sender, text);
                        Console.WriteLine(tweet.ToString());
                        Messages.Add(tweet);
                    } 
                    if(line[0].ToString().Equals("E"))
                    {
                        string id = line;
                        
                        string sender = rd.ReadLine();
                        if(!sender.ValidateEmailAdress())
                            throw new Exception("Invalid sender email address in Email with ID: " + id);

                        string subject = rd.ReadLine();
                        if(subject.Contains("SIR"))
                        {
                            if (!subject.ValidateDate())
                                throw new Exception("Invalid date in Email with ID: " + id);

                            string centreCode = rd.ReadLine();
                            if(!centreCode.ValidateCentreCode())
                                throw new Exception("Invalid Centre Code in Email with ID: " + id);

                            string incident = rd.ReadLine();
                            if(!incident.ValidateIncident())
                                throw new Exception("Invalid Nature of Incident in Email with ID: " + id);

                            string body = rd.ReadLine();
                            body = body.RemoveURLs(id);

                            EmailState sir = new EmailState(id, sender, subject, centreCode, incident, body);
                            Messages.Add(sir);
                            continue;
                        }                        
                        
                        string text = rd.ReadLine();
                        text = text.RemoveURLs(id);

                        EmailState email = new EmailState(id, sender, subject, text);
                        Console.WriteLine(email.ToString());
                        Messages.Add(email);                  
                    }
                }
            }
        }

       
    }
}