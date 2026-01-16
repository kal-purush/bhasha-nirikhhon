using System;
using System.Collections.Generic;
using System.Linq;

namespace MagicLandExplorer.Tasks
{
    public class FilterDestinations
    {
        public void Filter(List<Destination> destinations, int maxDuration)
        {
            var filteredDestinations = destinations
                .Where(d => !string.IsNullOrEmpty(d.Duration) &&
                            int.TryParse(d.Duration.Replace(" minutes", ""), out int duration) &&
                            duration < maxDuration)
                .ToList();

            foreach (var destination in filteredDestinations)
            {
                Console.WriteLine($"{destination.Name}");
            }
            Console.WriteLine();
        }
    }
}