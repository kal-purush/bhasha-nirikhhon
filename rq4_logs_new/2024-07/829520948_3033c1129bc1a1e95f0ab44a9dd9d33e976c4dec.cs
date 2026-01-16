using System.Xml.Linq;
using SystemFinder.Model.Data;

namespace SystemFinder.Abstractions.Logic.CampaignIO.Readers
{
    public interface IPlanetReader
    {
        void Read(XElement current, XAttribute uid, GalaxyData data);
    }
}