using EightCare.Domain.Common;
using EightCare.Domain.Enums;
using System.Collections.Generic;

namespace EightCare.Domain.KeeperAggregate
{
    public class Feed : ValueObject
    {
        public static Feed DubiaRoach = new Feed("Dubia Roach", FeedType.Alive);
        public static Feed MadagascarRoach = new Feed("Dubia Roach", FeedType.Alive);
        public static Feed TurkishRoach = new Feed("Dubia Roach", FeedType.Alive);

        public string Name { get; }
        public FeedType Type { get; }

        public Feed(string name, FeedType type)
        {
            Name = name;
            Type = type;
        }

        protected override IEnumerable<object> GetEqualityComponents()
        {
            yield return Name;
            yield return Type;
        }
    }
}