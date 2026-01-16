using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace esinibul
{
    public class Baglanti
    {
        public static string conn // connection için
        {
            get
            {
                // Not : MultipleActiveResultSets = True; -> dediğinizde -> tek bir cnn connection'da birden fazla dataReader çalıştırmak istediğinizde kullanabilirsiniz !!!

                // Şifresiz Bağlantı için;
                //return "Server = .; Database = Northwind; Integrated Security = True; MultipleActiveResultSets = True";

                // Şifreli Bağlantı için;
                return "Server=LENOVO; Database=EsiniBulDB; User Id=sa; Password=s";
            }
        }
    }
}