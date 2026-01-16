using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net;

namespace HumansInHarmony.Models
{
    public class ItunesDAL
    {

        public static SongInfo FindSong()
        {
            Random rnd = new Random();
            int songId = SongsArray.Songs[rnd.Next(0, 24)];

            HttpWebRequest request = WebRequest.CreateHttp($"https://itunes.apple.com/search?term={songId}");
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            StreamReader rd = new StreamReader(response.GetResponseStream());

            string APIText = rd.ReadToEnd();

            JToken token = JToken.Parse(APIText);

            SongInfo song = new SongInfo(token);

            return song;
        }
    }
}