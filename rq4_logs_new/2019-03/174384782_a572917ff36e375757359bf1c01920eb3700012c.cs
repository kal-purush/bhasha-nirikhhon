using System;
using System.Runtime.Serialization;

namespace TunesClient
{
    [DataContract]
    class Tunes
    {
        [DataMember(Name = "id")]
        public int Id { get; set; }
        [DataMember(Name = "irishChart")]
        public int IrishChart { get; set; }
        [DataMember(Name = "title")]
        public String Title { get; set; }
        [DataMember(Name = "artist")]
        public String Artist { get; set; }
        [DataMember(Name = "album")]
        public String Album { get; set; }
        [DataMember(Name = "genre")]
        public String Genre { get; set; }
        [DataMember(Name = "albumCoverLink")]
        public String AlbumCoverLink { get; set; }
        [DataMember(Name = "price")]
        public double Price { get; set; }
        [DataMember(Name = "buyLink")]
        public String BuyLink { get; set; }
        [DataMember(Name = "release")]
        public DateTime Realsed { get; set; }
        [DataMember(Name = "duration")]
        public String Duration { get; set; }

        public override string ToString()
        {
            return IrishChart + "\t" + Title + "\t" + Artist + "\t" + Album + "\t" + Genre;
        }
    }

    [DataContract]
    class SuggestedTunes
    {
        [DataMember(Name = "id")]
        public int Id { get; set; }
        [DataMember(Name = "count")]
        public int Count { get; set; }
        [DataMember(Name = "title")]
        public String Title { get; set; }
        [DataMember(Name = "artist")]
        public String Artist { get; set; }
        [DataMember(Name = "genre")]
        public String Genre { get; set; }

        public override string ToString()
        {
            return Count + "\t" + Title + "\t" + Artist + "\t" + Genre;
        }
    }
}
