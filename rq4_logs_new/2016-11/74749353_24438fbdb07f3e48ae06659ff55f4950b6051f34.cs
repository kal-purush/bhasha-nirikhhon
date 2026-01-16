using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fun12.Class
{
    class addsubproducten
    {
        string con = new db().connstring;

        private string _subnaam;
        private string _subomschrijving;

        public string subnaam
        {
            get { return this._subnaam; }
            set { this._subnaam = value; }
        }

        public string subomschrijving
        {
            get { return this._subomschrijving; }
            set { this._subomschrijving = value; }
        }
    }
}