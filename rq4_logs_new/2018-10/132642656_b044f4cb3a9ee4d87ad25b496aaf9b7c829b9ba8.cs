using System.Xml.Serialization;

namespace BggCoreSdk.Dto
{
    public class BoardGameDto
    {
        [XmlAttribute("objectid")]
        public string Id { get; set; }

        [XmlElement("yearpublished")]
        public string YearPublished { get; set; }

        [XmlElement("minplayers")]
        public string MinPlayers { get; set; }

        [XmlElement("maxplayers")]
        public string MaxPlayers { get; set; }

        [XmlElement("playingtime")]
        public string PlayingTime { get; set; }

        [XmlElement("minplaytime")]
        public string MinPlayTime { get; set; }

        [XmlElement("maxplaytime")]
        public string MaxPlayTime { get; set; }

        [XmlElement("age")]
        public string Age { get; set; }

        [XmlElement("name")]
        public BoardGameNameDto[] Names { get; set; }

        [XmlElement("description")]
        public string Description { get; set; }

        [XmlElement("thumbnail")]
        public string Thumbnail { get; set; }

        [XmlElement("image")]
        public string Image { get; set; }

        [XmlElement("boardgamesubdomain")]
        public ValueIdentifierDto[] BoardGameSubdomains { get; set; }

        [XmlElement("boardgamepublisher")]
        public ValueIdentifierDto[] BoardGamePublishers { get; set; }

        [XmlElement("boardgameversion")]
        public ValueIdentifierDto[] BoardGameVersions { get; set; }

        [XmlElement("boardgamecategory")]
        public ValueIdentifierDto[] BoardGameCategories { get; set; }

        [XmlElement("boardgameartist")]
        public ValueIdentifierDto[] BoardgameArtists { get; set; }

        [XmlElement("boardgameexpansion")]
        public ValueIdentifierDto[] BoardGameExpansions { get; set; }

        [XmlElement("boardgamedesigner")]
        public ValueIdentifierDto[] BoardGameDesigners { get; set; }

        [XmlElement("boardgamefamily")]
        public ValueIdentifierDto[] BoardGameFamilies { get; set; }

        [XmlElement("boardgameaccessory")]
        public ValueIdentifierDto[] BoardGameAccessories { get; set; }

        [XmlElement("videogamebg")]
        public ValueIdentifierDto[] VideogameBg { get; set; }

        [XmlElement("poll")]
        public BoardGamePollDto[] Polls { get; set; }
    }
}