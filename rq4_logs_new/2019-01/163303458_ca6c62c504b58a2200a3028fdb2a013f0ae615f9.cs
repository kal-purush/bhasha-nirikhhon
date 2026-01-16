using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BL_8_Struct_Enum_Tuple_Generic
{
    partial class Lesson
    {
        public static void AnanymousExample()
        {
            var song = new Anonymous.Song();

            //anonymous type
            var data = new
            {
                title = song.Title,
                author = song.AuthorName,
                album = song.Album,
                albumYear = song.Album.ReleaseDate.Year
            };

            Console.WriteLine($"{data.albumYear} - {song.Title} ({song.Album})");
        }  
        
        public static void ProcessListWithAnonymousExample(List<Anonymous.Song> songs, List<Anonymous.Author> authors)
        {
            var filteredSongs = songs
                .Where(s => !string.IsNullOrWhiteSpace(s.AuthorName))
                .Select(s => new
                {
                    Title = s.Title.ToUpper(),
                    Author = s.AuthorName,
                    Album = s.Album.Title
                })
                .Join(authors, (s => s.Author), (a => a.Name), (s, a) => new
                {
                    Title = s.Title,
                    Author = s.Author,
                    Album = s.Album,
                    Genre = a.Genre
                });

            foreach (var item in filteredSongs)
            {
                Console.WriteLine($"{item.Title} ({item.Album}, {item.Author}, {item.Genre})");
            }
        }        
    }    

    namespace Anonymous {

        public class Song
        {
            public string Title { get; set; }
            public int Duration { get; set; }
            public string AuthorName { get; set; }
            public Album Album { get; set; }
        }

        public class Author
        {
            public string Name { get; set; }
            public string Genre { get; set; }
        }

        public class Album
        {
            public string Title { get; set; }
            public DateTime ReleaseDate { get; set; }
        }
    }
}
