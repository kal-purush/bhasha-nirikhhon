using System;

namespace WordReader
{
    class Consultation
    {
        public string Lecturer { get; set; }
        public string Subject { get; set; }
        public string Group { get; set; }
        public string Date { get; set; }
        public string Time { get; set; }
        public string Place { get; set; }
        public string Addition { get; set; }

        public Consultation( string lecturer, string subject, string group, string date, string time,
                            string place, string addition)
        {
            Lecturer = lecturer;
            Subject = subject;
            Group = group;
            Date = date;
            Time = time;
            Place = place;
            Addition = addition;
        }
    }
}