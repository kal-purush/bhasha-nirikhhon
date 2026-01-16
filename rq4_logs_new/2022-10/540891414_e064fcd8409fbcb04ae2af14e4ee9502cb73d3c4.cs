using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterAPI.Domain.Model
{
    public class TweetHashtagsAPIModel
    {
        /// <summary>
        /// It gets or sets the hashtag primary key
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// It gets or set the tweet hashtags
        /// </summary>
        public string? Hashtag { get; set; }

        /// <summary>
        /// It gets or set the foreing key
        /// of the parent TweetId
        /// </summary>
        public int TweetId { get; set; }
    }
}