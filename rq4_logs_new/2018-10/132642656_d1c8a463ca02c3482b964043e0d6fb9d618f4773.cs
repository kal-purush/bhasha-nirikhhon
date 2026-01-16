using System.Xml.Serialization;

namespace BggCoreSdk.Dto
{
    public class BoardGamePollResultsDto
    {
        [XmlAttribute("numplayers")]
        public string NumPlayers { get; set; }

        [XmlElement("result")]
        public BoardGamePollResultDto[] ResultList { get; set; }
    }
}