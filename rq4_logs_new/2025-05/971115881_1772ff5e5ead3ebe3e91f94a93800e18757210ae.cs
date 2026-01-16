using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using TugasBesar_KPL_2425_Kelompok_4.Model;

namespace TugasBesar_KPL_2425_Kelompok_4.GarbageCollectionSchedule
{
    public class configPendaftaranPenjemputan<T>
    {
        private string configPath = "Riwayat_Pendaftaran_Penjemputan.json";
        public int id { get; set; }
        public string namaPengguna { get; set; }
        public configPendaftaraanArea Area { get; set; }
        public DateTime Jadwal { get; set; }
        public T KeteranganTambahan { get; set; }

        public void Simpan()
        {
            List<configPendaftaranPenjemputan<T>> data = new();

            if (File.Exists(configPath))
            {
                string content = File.ReadAllText(configPath);

                // Validasi isi file, hanya deserialisasi jika isi valid
                if (!string.IsNullOrWhiteSpace(content))
                {
                    try
                    {
                        data = JsonSerializer.Deserialize<List<configPendaftaranPenjemputan<T>>>(content)
                            ?? new List<configPendaftaranPenjemputan<T>>();
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine("⚠️ Gagal membaca file JSON: " + ex.Message);
                        // Bisa reset atau abaikan data lama
                    }
                }
            }
            int maxId = 0;
            foreach (var item in data)
            {
                if (item.id > maxId)
                {
                    maxId = item.id;
                }
            }
            this.id = maxId + 1;
            data.Add(this);

            var serialized = JsonSerializer.Serialize(data, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(configPath, serialized);
            Console.WriteLine("Pendaftaran berhasil disimpan.");

        }

    }
}