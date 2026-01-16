using System.Collections.Generic;

namespace Shared.JsonObjects
{
    public class CrewJson
    {
        public int Id { get; set; }

        public List<PilotJson> Pilot { get; set; }

        public List<StewardessJson> Stewardess { get; set; }
    }
}