using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ODWai2.ODWaiCore.Models
{
    public class Field
    {
        public string field_name;
        public string[] associated;
        public string[] sample;
        public string[] error;

        public static Field new_field(string _name, string _associated, string _sample, string _error)
        {
            string[] __associated = _associated.Split(',');
            string[] __sample = _sample.Split(',');
            string[] __error = _error.Split(',');
            for (int i = 0; i < __associated.Length; ++i) { __associated[i] = __associated[i].Trim(); }
            for (int i = 0; i < __sample.Length; ++i) { __sample[i] = __sample[i].Trim(); }
            for (int i = 0; i < __error.Length; ++i) { __error[i] = __error[i].Trim(); }
            return new Field { field_name = _name, associated = __associated, sample =__sample, error = __error };
        }
    }
}