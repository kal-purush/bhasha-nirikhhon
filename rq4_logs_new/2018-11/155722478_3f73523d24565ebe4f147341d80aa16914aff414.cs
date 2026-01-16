using System;
using System.Diagnostics;
using MovieRating_Compulsary;
using Xunit;

namespace XUnitTestProject1
{
    public class PerformanceTest
    {
        public PerformanceTest()
        {
            _movieRatingService.LoadJson();
        }

        private const int ReviewerIdTest = 1;
        private const int SpecificGradeTest = 1;
        private const int MovieIdTest = 1;
        private readonly IMovieRatingService _movieRatingService = new MovieRatingService();
        /**
         *  Running the unit test takes a while at the start
         *  Due to the fact that it has the load the JSONFile
         */
        
        
        //1 
        [Fact]
        public void ReviewersTotalRatingsTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.ReviewersTotalRatings(ReviewerIdTest);

            sw.Stop();
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //2
        [Fact]
        public void ReviewersAverageGradeTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.ReviewersAverageGrade(ReviewerIdTest);
            
            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
            
        }   
        
        //3
        [Fact]
        public void ReviewersSpecificGradingTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.ReviewersSpecificGrading(ReviewerIdTest, SpecificGradeTest);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        //4
        [Fact]
        public void HowManyTimesHasMovieBeenReviewedTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.MovieAmountOfReviews(MovieIdTest);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //5   
        [Fact]
        public void AverageGradeOnMovieTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.AverageGradeOfMovie(1488844);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //6
        [Fact]
        public void HowManyTimesHasMovieReceivedSpecificGradeTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.HowManyTimesHasMovieReceivedSpecificGrade(MovieIdTest, SpecificGradeTest);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //7 
        [Fact]
        public void MoviesWithMostRatingsOfFiveTest()        
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.MoviesWithMostRatingsOfFive();

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //8
        [Fact]
        public void ReviewerWithMostReviewsTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.ReviewerWithMostRatings();

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //9
        [Fact]
        public void FindTopXOfMoviesTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.FindTopXOfMovies(5);

            Assert.True(sw.Elapsed < new TimeSpan(0,0,0,4,0),
                "The function took 4 seconds or more.");
        }
        
        //10
        [Fact]
        public void WhatMoviesHasXReviewedTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.WhatMoviesHasXRated(ReviewerIdTest);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
        
        //11 
        [Fact]
        public void WhatReviewersHasReviewedXMovieTest()
        {
            var sw = new Stopwatch();
            sw.Start();

            _movieRatingService.WhatReviewersHasRatedXMovie(MovieIdTest);

            
            Assert.True(sw.Elapsed < new TimeSpan(0, 0, 0, 4, 0),
                "The function took 4 seconds or more.");
        }
    }
}