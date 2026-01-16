namespace MetalVerseBackend.Models
{
    public class StreamMetadata
    {
        // these will hold data that is of interest to us
        public string SongTitle { get; set; }
        public string Artist { get; set; }
        public string AlbumCover { get; set; }


        // data directly fetched from JSON
        public Data data { get; set; }

        public class Data
        {
            public string artist { get; set; }
            public string title { get; set; }
            public Cover cover { get; set; }
        }

        public class Cover
        {
            public string baseurl { get; set; }
            public string[] sizes { get; set; }
            public string filename { get; set; }
            public Dictionary<string, string> extensions { get; set; }

            public string GetCoverImageUrl()
            {
                return $"{baseurl}{sizes[0]}/{filename}{extensions["image/webp"]}";
            }
        }
    }
}