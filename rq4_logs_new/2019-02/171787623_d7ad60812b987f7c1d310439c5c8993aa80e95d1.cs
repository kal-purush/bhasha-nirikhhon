using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Threading;
using System.Diagnostics;
using Tweetinvi;

namespace cpsc571.Helpers
{
    public class TwitterStream
    {

        private bool _stopThread = false;
        public List<Tweetinvi.Models.ITweet> tweetsList = new List<Tweetinvi.Models.ITweet>();

        public void StartStream()
        {
            var stream = Stream.CreateFilteredStream();
            stream.AddTrack("dog");

    
            stream.MatchingTweetReceived += (sender, args) =>
            {
                tweetsList.Add(args.Tweet);
                var tweet = args.Tweet;
                var matchingTracks = args.MatchingTracks;
                var matchedOn = args.MatchOn;
                Debug.WriteLine(args.Tweet);
            };
            
            stream.StartStreamMatchingAllConditionsAsync();
        }

        public void StopStream()
        {
            _stopThread = true;
        }

        public List<Tweetinvi.Models.ITweet> GetListTweets()
        {
            List<Tweetinvi.Models.ITweet> returnTweets = new List<Tweetinvi.Models.ITweet>(tweetsList);
            tweetsList.Clear();
            return returnTweets;
        }
    }
}