using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Models;
namespace TechTitans.Views.Components.User
{
    public partial class UserSongDashboard : ContentPage 
    {
        // alt domain song type cu mai multe detalii
        SongBasicInfo song;
        public UserSongDashboard(SongBasicInfo song)
        {
            // song = service.GetSongById(songId);
            this.song = song;
            InitializeComponent();
            LoadPage();
        }

        private void LoadPage()
        {
            Console.WriteLine(song);
            SongImageUrl.Source = song.Image;
            SongTitle.Text = song.Name;
            SongGenre.Text = song.Genre;
            SongSubGenre.Text = song.Subgenre;
            SongArtist.Text = song.Artist;
            SongCountry.Text = song.Country;
            SongLanguage.Text = song.Language;
            SongAlbum.Text = song.Album;

        }

        private SongBasicInfo getMockedSong()
        {
            return new SongBasicInfo();
        }
    }
}