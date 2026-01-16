using System;

namespace Exercise4
{
    public class Movie
    {
        public string name;
        public string studio;
        public string rating;

        public Movie(string name, string studio, string rating)
        {
            this.name = name;
            this.studio = studio;
            this.rating = rating;
        }
        public Movie(string name, string studio)
        {
            this.name = name;
            this.studio = studio;
            this.rating = "PG";
        }
        public static Movie[] GetPG(Movie[] movies)
        {
            int index = 0;
            Movie[] result = new Movie[index];
            foreach (Movie i in movies)
            {              
                if (i.rating == "PG") {                 
                    Array.Resize(ref result, ++index);
                    result[index - 1] = i;                   
                }               
            }
            return result;
        }
    }

    
}