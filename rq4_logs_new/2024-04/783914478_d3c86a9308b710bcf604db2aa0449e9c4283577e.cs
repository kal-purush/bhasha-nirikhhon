using LogicLayer.Models;

namespace LogicLayer.Core;

public static partial class Core
{
    public static Section? GetSection(Section section)
    {
        CheckInit();
        
        return _sectionService.GetFromKey(section).GetAwaiter().GetResult();
    }
    
    public static List<Section>? GetAllSections()
    {
        CheckInit();
        
        return _sectionService.GetAll().GetAwaiter().GetResult();
    }
    
    public static List<Unit> GetUnitsInSection(Section section)
    {
        CheckInit();

        var units = GetAllUnits();

        return units != null ? units.Where(unit => unit.SectionId == section.Id).ToList() : [];
    }
}