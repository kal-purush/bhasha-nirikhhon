using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain.Entities.Imported_Entities.Import_TournamentEntities
{
    public class Import_TournamentEntity
    {
        public string Name { get; set; }
        public string? OverviewPage { get; set; }
        public DateTime? DateStart { get; set; }
        public DateTime? Date { get; set; }
        public string? DateStartFuzzy { get; set; }
        public string? League { get; set; }
        public string? Region { get; set; }
        public string? Country { get; set; }
        public string? ClosestTimezone { get; set; }
        public string? EventType { get; set; }
        public string? StandardName { get; set; }
        public string? Split { get; set; }
        public int? SplitNumber { get; set; }
        public string? SplitMainPage { get; set; }
        public string? TournamentLevel { get; set; }
        public bool? IsQualifier { get; set; }
        public bool? IsPlayoffs { get; set; }
        public bool? IsOfficial { get; set; }
        public string? Year { get; set; }
        public string? AlternativeNames { get; set; }
        public string? Tags { get; set; }
    }
}