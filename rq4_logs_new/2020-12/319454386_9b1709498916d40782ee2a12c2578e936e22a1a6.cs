using DotNetTeam7API.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DotNetTeam7API.ViewModels
{
    public class MovieIndexVM
    {
        public List<MovieVM> Movies { get; set; }
        public List<Genre> Genres { get; set; }
}
}