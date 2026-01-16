using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using mbl = MovieBusinessLayer.MovieBusinessLayer;

namespace WebMovies
{
    public partial class addFilm : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void btn_CreateUpdate_Click(object sender, EventArgs e)
        {
            mbl bl1 = new mbl();
            List<string> inputData = new List<string>()
            {
                tb_FilmImdbID.Text,
                tb_FilmName.Text,
                tb_FilmImdbRating.Text,
                tb_DirectorImdbID.Text,
                tb_DirectorName.Text,
                tb_ActorImdbID.Text,
                tb_ActorName.Text,
                tb_FilmYear.Text
            };
            bl1.UpdateFilmInDatabase(inputData);
        }
    }
}