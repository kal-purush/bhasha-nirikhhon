import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
// IMPORT LIBRARY PACKAGES NEEDED BY YOUR PROGRAM
// SOME CLASSES WITHIN A PACKAGE MAY BE RESTRICTED
// DEFINE ANY CLASS AND METHOD NEEDED
// CLASS BEGINS, THIS CLASS IS REQUIRED
class Solution
{
    // METHOD SIGNATURE BEGINS, THIS METHOD IS REQUIRED
    HashMap<String, List<String>> favoriteVideoGenre(int numUsers,HashMap<String, List<String>> userVideosWatched, int numGenres,HashMap<String, List<String>> videoGenres)
    {
        HashMap<String, List<String>> result = new HashMap<String, List<String>>();

		Map<String,String> videoToGenre = new HashMap<String, String>();

		for(String genre: videoGenres.keySet()) {
			for(String video : videoGenres.get(genre)) {
				videoToGenre.put(video,genre);
			}
		}

		Map<String, Map<String, Integer>> userGenre = new HashMap<String, Map<String,Integer>>();
		for(String user : userVideosWatched.keySet()) {
			Map<String, Integer> genreCount = new HashMap<String, Integer>();
			userGenre.put(user,genreCount);
			int max = -1;
			for(String video : userVideosWatched.get(user)) {
				if(videoToGenre.containsKey(video)) {
					String genre = videoToGenre.get(video);
					if(genreCount.containsKey(genre)) {
						genreCount.put(genre,genreCount.get(genre) + 1);
					} else {
						genreCount.put(genre,1);
					}
					max = Math.max(genreCount.get(genre),max);

				}
			}
			ArrayList<String> genreList = new ArrayList<String>();
			result.put(user,genreList);
			if(max >= 0) {
				for(String genre : genreCount.keySet()) {
					if(genreCount.get(genre).intValue() == max) {
						genreList.add(genre);
					}
				}
			}
		}
		return result;
    }
    // METHOD SIGNATURE ENDS
}

SDE-university-assessment@amazon.com