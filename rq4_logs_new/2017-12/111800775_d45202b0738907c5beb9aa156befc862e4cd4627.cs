using FilmDTOLibrary;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SmartVideoWpf
{
    class FilmsViewModel
    {
        private ServiceReference1.FilmDTO _Data;

        public FilmsViewModel(ServiceReference1.FilmDTO data)
        {
            _Data = data;
        }

        public string Name
        {
            get { return _Data.Original_title; }
            set { _Data.Original_title = value; }
        }



    }
}