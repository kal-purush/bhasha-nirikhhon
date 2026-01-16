using System;

namespace Hippologamus.Server.Models
{
    public class ErrorLogList
    {
        public int Id { get; set; }
        public string Level { get; set; }
        public DateTime TimeStamp { get; set; }
        public string Properties { get; set; }
        public string LogEvent { get; set; }
        public string MachineName { get; set; }
        public string Assembly { get; set; }
        public string Message { get; set; }
        public string ExceptionMessage { get; set; }
        public string RequestPath { get; set; }
    }
}