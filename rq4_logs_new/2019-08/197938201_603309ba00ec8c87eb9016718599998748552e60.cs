using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UrlaubsPlaner.Controller
{
    public static class CommonController
    {
        public static string ButtonTextChange(bool value) => value ? "Erstellen" : "Aktualisieren";
    }
}