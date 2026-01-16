namespace modelLibrary
{
    public class Jadwal<T>
    {
        public DateOnly Tanggal { get; set; }
        public List<T> JenisSampahList { get; set; }
        public pengguna KurirPengambil { get; set; }
        public string AreaDiambil { get; set; }

        public Jadwal(DateOnly tanggalInput, List<T> jenisSampahListInput, string areaDiambilInput, pengguna kurirPengambilInput)
        {
            Tanggal = tanggalInput;
            JenisSampahList = jenisSampahListInput;
            KurirPengambil = kurirPengambilInput;
            AreaDiambil = areaDiambilInput;
        }
    }
}