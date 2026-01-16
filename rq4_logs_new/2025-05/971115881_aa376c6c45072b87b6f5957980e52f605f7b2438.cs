using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace TugasBesar_KPL_2425_Kelompok_4.Model
{
    class Jadwal <T>
    {
        public DateTime Tanggal { get; set; }
        public List<T> jenisSampahList { get; set; }
        public pengguna kurirPengambil { get; set; }

        public Jadwal(DateTime tanggalInput, List<T> jenisSampahListInput, pengguna kurirPengambilInput)
        {
            Debug.Assert(jenisSampahListInput != null, "Jenis Sampah yang diambil tidak boleh kosong");
            if (jenisSampahListInput == null)
            {
                throw new ArgumentNullException(nameof(jenisSampahListInput), "Jenis Sampah yang diambil tidak boleh kosong");
            }

            Debug.Assert(kurirPengambilInput != null, "Jadwal harus memiliki kurir");
            if (kurirPengambilInput == null)
            {
                throw new ArgumentNullException(nameof(kurirPengambilInput), "Jadwal harus memiliki kurir");
            }

            Debug.Assert(kurirPengambilInput.peran == JenisPengguna.Kurir, "Hanya Kurir yang bisa mengambil sampah");
            if (kurirPengambilInput.peran != JenisPengguna.Kurir)
            {
                throw new ArgumentException("Hanya Kurir yang bisa mengambil sampah");
            }

            Debug.Assert(tanggalInput >= DateTime.Now.Date, "Tanggal tidak boleh Berada di masa lalu");
            if (tanggalInput < DateTime.Now.Date)
            {
                throw new ArgumentOutOfRangeException(nameof(tanggalInput), "Tanggal tidak boleh berada di masa lalu");
            }

            Debug.Assert(tanggalInput != null, "Tanggal tidak boleh kosong");
            if (tanggalInput == null)
            {
                throw new ArgumentNullException(nameof(tanggalInput), "Tanggal tidak boleh kosong");
            }

            Tanggal = tanggalInput;
            jenisSampahList = jenisSampahListInput;
            kurirPengambil = kurirPengambilInput;
        }

        public Jadwal()
        {
            
        }
    }
}