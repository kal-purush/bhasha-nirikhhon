using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Entity;
using cpsc571.Models;

namespace cpsc571.DAL
{
    public class DataInitializer : DropCreateDatabaseAlways<TwitterDbContext>
    {
        protected override void Seed(TwitterDbContext context)
        {
            List<Tweet> tweets = new List<Tweet>
            {
                new Tweet {FullText = "This is tweet 1"},
                new Tweet {FullText = "This is tweet 2"}
            };
            tweets.ForEach(t => { context.Tweets.Add(t); });
        }
    }
}