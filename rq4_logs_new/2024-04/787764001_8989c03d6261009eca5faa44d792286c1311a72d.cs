
namespace TechTitans.Models
{
    public class SongBasicInfo
    {
        public int SongId { get; set; }
        public string Name { get; set; }
        public string Genre { get; set; }
        public string Subgenre { get; set; }
        public string Artist { get; set; }
        public IList<string> Features { get; set; }
        public string Language { get; set; }
        public string Country { get; set; }
        public string Album { get; set; }
        public string Image {  get; set; }
    }
}