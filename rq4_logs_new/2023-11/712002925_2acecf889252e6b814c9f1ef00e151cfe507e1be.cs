using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using FalknerCountyJuvenileCourt.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;

public class ReportModel : PageModel
{
    private readonly CourtContext _context;

    public ReportModel(CourtContext context)
    {
        _context = context;
    }

    [IgnoreAntiforgeryToken]
    public JsonResult OnGetRaceDistributionData()
    {
        try
        {
            var crimes = _context.Crimes
                .Include(c => c.Juvenile)
                .ThenInclude(j => j.Race)
                .ToList();

            var raceCounts = crimes
                .Select(c => c.Juvenile?.Race?.Name)
                .Where(race => !string.IsNullOrEmpty(race))
                .GroupBy(race => race)
                .Select(group => new { Race = group.Key, Count = group.Count() })
                .ToList();

            return new JsonResult(raceCounts);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            return new JsonResult("An error occurred while processing the data.");
        }
    }
}