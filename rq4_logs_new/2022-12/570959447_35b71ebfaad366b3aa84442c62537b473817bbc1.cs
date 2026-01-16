using Lyricist.ViewModel;
using Lyricist.Model;

namespace LyricsAppTest
{
    public class UnitTest1
    {
        [Fact]
        
        public void ConcatArtistName()
        {


            //arrange
            Music Mock1 = new Music("team", "j. cole", "rap", "Lyrics1");
            //This test is to check if the artist name was successfully concatinated with a string
            //evn though only the name was entered
            String expected = "Artist: j. cole";
            //act

            String actual = Mock1.Artist;

            //assert

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void ConcatGenre()
        {

            //arrange
            Music Mock1 = new Music("team", "j. cole", "rap", "Lyrics1");
            //This test is to check if the song genre was successfully concatinated with a string
            //evn though only the genre was entered
            String expected = "Genre: rap";
            //act

            String actual = Mock1.Genre;

            //assert

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestNewMusicEmptyConstructor() {
            //arrange
            Music Mock1 = new Music();
            //This test is to check if an instance of music can be created with just an empty constructor
            String expected = null;
            //act

            String actual = Mock1.Title;

            //assert

            Assert.Equal(expected, actual);
        }


    }
}