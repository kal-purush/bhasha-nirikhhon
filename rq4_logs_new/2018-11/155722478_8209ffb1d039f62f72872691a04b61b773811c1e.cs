using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using System.Linq;

namespace MovieRating_Compulsary
{
    public class MovieRatingService: IMovieRatingService
    {
        private const string JsonPath = "/Users/choi/Desktop/ratings.json"; //ENTER PATH HERE TO THE JSON FILE
        
        private static List<MovieRatingEntity> ratings;

        public void LoadJson()
        {
            if (ratings == null)
            {
                using (var reader = new StreamReader(JsonPath))
                {
                    var jsonFile = reader.ReadToEnd();
                    ratings = JsonConvert.DeserializeObject<List<MovieRatingEntity>>(jsonFile);
                }
            }
        }
        
        //1
        public int ReviewersTotalRatings(int x)
        {
            return ratings.Count(i => i.Reviewer == x);
        }
    }
}