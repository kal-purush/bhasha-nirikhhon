using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TugasBesar_KPL_2425_Kelompok_4.Model;

namespace TugasBesar_KPL_2425_Kelompok_4.GarbageCollectionSchedule
{
    static class rulesJadwal
    {
        public static readonly Dictionary<JenisSampah, DayOfWeek[]> HariPengambilan = new()
        {
            { JenisSampah.Organik, new[] { DayOfWeek.Monday, DayOfWeek.Wednesday } },
            { JenisSampah.Plastik, new[] { DayOfWeek.Tuesday, DayOfWeek.Thursday } },
            { JenisSampah.Kertas, new[] { DayOfWeek.Wednesday, DayOfWeek.Friday } },
            { JenisSampah.Logam, new[] { DayOfWeek.Thursday, DayOfWeek.Saturday } },
            { JenisSampah.Elektronik, new[] { DayOfWeek.Friday, DayOfWeek.Sunday } },
            { JenisSampah.BahanBerbahaya, new[] { DayOfWeek.Saturday, DayOfWeek.Monday } },
            { JenisSampah.Minyak, new[] { DayOfWeek.Sunday, DayOfWeek.Tuesday } } 
        };

        public static bool pengambilanValidasi(JenisSampah jenis, DateTime tanggal)
        {
            Debug.Assert(HariPengambilan != null, "Aturan Tidak Boleh Kosong");
            return HariPengambilan.TryGetValue(jenis, out var hari) && hari.Contains(tanggal.DayOfWeek);
        }
    }
}