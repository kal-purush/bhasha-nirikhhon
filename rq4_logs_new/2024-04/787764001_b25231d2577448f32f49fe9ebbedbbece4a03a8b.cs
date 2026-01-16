using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Models;
using TechTitans.Repositories;
namespace TechTitans.Services
{
    public class TopGenresController
    {
        private Repository<SongBasicDetails> SongRepo = new Repository<SongBasicDetails>();
        private Repository<SongRecommendationDetails> SongRecommendationRepo= new Repository<SongRecommendationDetails>();

        public void getTop3Genres(int month,int year) {
            Dictionary<String, int> genreCount = new Dictionary<String, int>();
            foreach (SongBasicDetails song in SongRepo.GetAll()) { 
                foreach (SongRecommendationDetails songDetails in SongRecommendationRepo.GetAll())
                {
                    if (songDetails.SongId == song.Song_Id && songDetails.Month == month && songDetails.Year == year)
                    {
                        if (genreCount.ContainsKey(song.Genre))
                        {
                            genreCount[song.Genre] += songDetails.Minutes_Listened;
                        }
                        else
                        {
                            genreCount.Add(song.Genre, songDetails.Minutes_Listened);
                        }
                    }
                }
            }

            var sortedDict = from entry in genreCount orderby entry.Value descending select entry;
            int count = 0;
            foreach (KeyValuePair<String, int> entry in sortedDict)
            {
                if (count == 3)
                {
                    break;
                }
                Console.WriteLine(entry.Key);
                count++;
            }
            
        }

    }

    
}