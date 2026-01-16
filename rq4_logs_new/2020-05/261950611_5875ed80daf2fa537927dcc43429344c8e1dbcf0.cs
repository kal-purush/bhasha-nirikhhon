using System;
using System.IO;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;

class day4
{
    public static string result;
    //test for global Variable
    public static void Main()
    {
        string url = @"https://www.microsoft.com/en-us/translator/business/languages/
";

        WebClient client = new WebClient();
        client.Credentials = CredentialCache.DefaultCredentials;
        byte[] data = client.DownloadData(url);
        string html = Encoding.UTF8.GetString(data);
        //Console.WriteLine(html);
        //debug
        List<row> textList = GetContent(html);
        /*foreach (row line in textList) { 
            Console.WriteLine(line.NeuralTranslation);
        }*/
        //debug
        using (var writer = new StreamWriter("/Users/admin/Desktop/practice/result.csv"))
        {
            foreach (row each in textList)
            {
                var line = string.Format("{0},{1},{2},{3},{4},{5}", each.TranslatorLanguage, each.NeuralTranslation, each.Customization, each.Transliteration, each.Dictionary, each.SpeechTranslation);
                writer.WriteLine(line);
                writer.Flush();
            }
        }
    }

    public struct row
    {
        public string TranslatorLanguage;
        public bool NeuralTranslation;
        public bool Customization;
        public bool Transliteration;
        public bool Dictionary;
        public bool SpeechTranslation;
    }
    public static List<row> GetContent(string str)
    {

        string tmpStr = string.Format("<tbody[^>]*?>(?<Text>[^*]*)</tbody>");
        //get the value in input label
        string trStr = string.Format("<tr[^>]*?>(?<Text>[^*]*?)</tr>");
        //get the value in tr label
        List<string> languageName = new List<string>();
        List<row> text = new List<row>();
        MatchCollection tbodyCollection = Regex.Matches(str, tmpStr, RegexOptions.IgnoreCase);
        foreach (Match match in tbodyCollection)
        {
            result = match.Groups["Text"].Value;
        }
        //Console.WriteLine(result);
        MatchCollection trCollection = Regex.Matches(result, trStr, RegexOptions.IgnoreCase);
        foreach (Match match in trCollection)
        {
            string temp = match.Groups["Text"].Value;
            string row_ = Regex.Replace(temp, @"<td><\/td>", "No.");
            //replace space to No
            row_ = Regex.Replace(row_, @"<sup>([^*]*?)<\/sup>", "");
            //delete sup label
            row_ = Regex.Replace(row_, @"<[^*]*?>", ".");
            //delete all labels
            string[] each_row = Regex.Split(row_, @"\.+");
            each_row = each_row.Where(x => !string.IsNullOrEmpty(x)).ToArray();
            //Console.WriteLine(each_row[0]);
            row row_line = new row();
            row_line.TranslatorLanguage = each_row[0];
            row_line.NeuralTranslation = Judge(each_row[1]);
            row_line.Customization = Judge(each_row[2]);
            row_line.Transliteration = Judge(each_row[3]);
            row_line.Dictionary = Judge(each_row[4]);
            row_line.SpeechTranslation = Judge(each_row[5]);
            text.Add(row_line);
        }
        return text;
    }

    public static bool Judge(string sentence)
    {
        if (sentence == "Yes")
        {
            return true;
        }
        else
        {
            return false;
        }
    }
}