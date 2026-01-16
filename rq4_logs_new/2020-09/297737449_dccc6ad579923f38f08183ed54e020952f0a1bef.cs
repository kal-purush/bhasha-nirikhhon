using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Course3
{
    public class MailService
    {
        private string _name;
        private string _url;
        private int _port;

        public string Name
        {
            get => _name;
            private set => _name = value;
        }
        public string Url
        {
            get => _url;
            private set => _url = value;
        }
        public int Port
        {
            get => _port;
            private set => _port = value;
        }

        Regex reg = new Regex(@"(?<name>\w*)\s(?<url>\w*\.\w*\.\w*)\s(?<port>\d*)", RegexOptions.Compiled);

        public MailService(string input)
        {
            var match = reg.Match(input);
            if (match == null) return;
            Name = match.Groups["name"].Value;
            Url = match.Groups["port"].Value;
            Port = Convert.ToInt32(match.Groups["port"].Value);
        }
        
    }
}