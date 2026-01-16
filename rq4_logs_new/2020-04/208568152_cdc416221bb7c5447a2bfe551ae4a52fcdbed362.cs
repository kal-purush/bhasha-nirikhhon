using System;
using AtmApp.API.Models;

namespace AtmApp.API.Dtos
{
    public class FaultLogForCreateDto
    {
          public int Id { get; set; }
        public int AtmFleetId {get; set;}
        public string TerminalId { get; set; }
        public string TerminalName { get; set; }    
        public string Vendor { get; set; }
        public string Brand { get; set; }   
        public string NatureOfFault { get; set; }
        public string CustodianName { get; set; }
        public string CustodianNumber { get; set; }
        public DateTime DateLogged { get; set; }
        public DateTime DateResolved { get; set; }
        public TimeSpan DefaultDays { get; set; }
    }
}