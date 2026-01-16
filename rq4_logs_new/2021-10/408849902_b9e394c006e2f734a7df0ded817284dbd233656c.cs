using System.Collections.Generic;

namespace SosCentar.Contracts.Dtos.ReportTables
{
    public class ExportReportDto
    {
        public string Title { get; set; }
        public string SubTitle { get; set; }
        public IEnumerable<ExportSectionDto> Sections { get; set; } = new List<ExportSectionDto>();
    }
}