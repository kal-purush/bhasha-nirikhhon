using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ResponsiPemrograman4297
{
    class Karyawan
    {
        public int Nik;
        public string Nama;
        public int GajiBulanan;
        public int GajiNaik;
        


        public Karyawan(int NIK, string NAMA, int GAJIBULANAN)
        {
            Nik = NIK;
            Nama = NAMA;
            GajiBulanan = GAJIBULANAN;
            GajiNaik = 10;

            if (GAJIBULANAN < 0)
            {
                GajiBulanan = 0;
            }
        }

        public void getdapetbonus()
        {
            int bonus = GajiBulanan / GajiNaik;
            GajiBulanan = GajiBulanan + bonus;
            Console.WriteLine("{0}  \t{1}\t\t\t {2}", Nik, Nama, GajiBulanan);
        }



        public void PrintAndShow()
        {
            Console.WriteLine("{0}  \t{1}\t\t\t {2}", Nik, Nama, GajiBulanan);
        }


    }
}