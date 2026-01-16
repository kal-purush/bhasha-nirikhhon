using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Infrastructure;
using WolfDen.Application.DTOs.Attendence;

namespace WolfDen.Application.Requests.Queries.Attendence.MonthlyReport
{
    public class MonthlyPdf
    {
        public IDocument CreateMonthlyReportDocument(MonthlyReportDTO monthlyReportDTO)
        {
            return Document.Create(container =>
            {
                container.Page(page =>
                {
                    page.Size(PageSizes.A4);
                    page.Margin(2, Unit.Centimetre);
                    page.PageColor(Colors.White);
                    page.DefaultTextStyle(x => x.FontSize(12));

                    page.Header()
                        .Text("Monthly Attendance Report")
                        .AlignCenter()
                        .SemiBold().FontSize(30).FontColor(Colors.Black);

                    page.Content()
                        .PaddingVertical(1, Unit.Centimetre)
                        .Column(column =>
                        {
                            column.Item().PaddingLeft(2, Unit.Centimetre).Column(col =>
                            {
                                col.Item().Row(row =>
                                {
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"Present Days: {monthlyReportDTO.Present}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"Absent Days: {monthlyReportDTO.Absent}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                });
                                col.Item().Row(row =>
                                {
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"Incomplete Shifts: {monthlyReportDTO.IncompleteShift}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"Holiday Days: {monthlyReportDTO.Holiday}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                });
                                col.Item().Row(row =>
                                {
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"WFH Days: {monthlyReportDTO.WFH}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                    row.RelativeItem().Padding(5).AlignLeft().Text($"Leave Days: {monthlyReportDTO.Leave}")
                                        .SemiBold().FontSize(14).FontColor(Colors.Black);
                                });
                            });

                            column.Item().PaddingVertical(1, Unit.Centimetre).Table(table =>
                            {
                                table.ColumnsDefinition(columns =>
                                {
                                    columns.ConstantColumn(180); 
                                    columns.RelativeColumn();   
                                });

                                table.Header(header =>
                                {
                                    header.Cell().Border(1).BorderColor(Colors.Black)
                                          .Background(Colors.Grey.Lighten3)
                                          .Padding(10).AlignCenter().Text("Category").Bold().FontSize(12);
                                    header.Cell().Border(1).BorderColor(Colors.Black)
                                          .Background(Colors.Grey.Lighten3)
                                          .Padding(10).AlignCenter().Text("Details").Bold().FontSize(12);      
                                });

                                table.Cell().Border(1).Padding(10).Text("Incomplete Shift Days").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.IncompleteShiftDays).FontSize(12);

                                table.Cell().Border(1).Padding(10).Text("Normal Holidays").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.NormalHolidays).FontSize(12);

                                table.Cell().Border(1).Padding(10).Text("Restricted Holidays").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.RestrictedHolidays).FontSize(12);

                                table.Cell().Border(1).Padding(10).Text("WFH Days").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.WFHDays).FontSize(12);

                                table.Cell().Border(1).Padding(10).Text("Leave Days").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.LeaveDays).FontSize(12);

                                table.Cell().Border(1).Padding(10).Text("Absent Days").FontSize(12);
                                table.Cell().Border(1).Padding(10).Text(monthlyReportDTO.AbsentDays).FontSize(12);

                            });
                        });

                    page.Footer()
                        .AlignCenter()
                        .Text(x =>
                        {
                            x.Span("Page ");
                            x.CurrentPageNumber();
                        });
                });
            });
        }
    }
}