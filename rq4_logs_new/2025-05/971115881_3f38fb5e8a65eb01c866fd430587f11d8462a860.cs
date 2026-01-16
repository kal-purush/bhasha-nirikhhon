using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TugasBesar_KPL_2425_Kelompok_4
{
    class Menu
    {
        public static void menuAdmin(string username)
        {
            Console.WriteLine("Selamat Datang, " + username);
            Console.WriteLine("(1) Menjadwalkan pengambilan sampah");
            Console.WriteLine("(2) Melihat jadwal pengambilan sampah");
            Console.WriteLine("(3) Mengubah jadwal pengambilan sampah");
            Console.WriteLine("(4) Menghapus jadwal pengambilan sampah");
            Console.WriteLine("(5) Keluar");
            Console.WriteLine("Pilih Menu : ");
        }

        public static void menuUser(string username)
        {
            Console.WriteLine("Selamat Datang, " + username);
            Console.WriteLine("(1) Melihat jadwal pengambilan sampah");
            Console.WriteLine("(2) Menarik keuntungan dari sampah");
            Console.WriteLine("(4) Mendaftarkan area pengambilan sampah");
            Console.WriteLine("(5) Mendaftarkan pengambilan sampah");
            Console.WriteLine("(6) Keluar");
            Console.WriteLine("Pilih Menu : ");
        }

        public static void menuKurir(string username)
        {
            Console.WriteLine("Selamat Datang, " + username);
            Console.WriteLine("(1) Melihat jadwal pengambilan sampah");
            Console.WriteLine("(2) Keluar");
            Console.WriteLine("Pilih Menu : ");
        }
    }
}