using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace SantexLeague.Common.Exceptions
{
    public class StatusMessage
    {
        public StatusMessage(string message)
        {
            this.Message = message;
        }
        public string Message { get; set; }
       
    }
}