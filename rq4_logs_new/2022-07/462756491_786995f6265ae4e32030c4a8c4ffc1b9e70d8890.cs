using CthulhuWizard.Persistence.DefaultData.Occupations;
using CthulhuWizard.Persistence.Models.Occupations;

namespace CthulhuWizard.Persistence.DefaultData;

public class OccupationsFactory {
    public static List<OccupationEntity> GetAll() {
        var types = typeof(IOccupationFactory).Assembly
            .GetTypes()
            .Where(p => typeof(IOccupationFactory).IsAssignableFrom(p) && !p.IsInterface);
        return types
            .Select(a => ((IOccupationFactory) Activator.CreateInstance(a)!).Create())
            .ToList()!;
    }
}