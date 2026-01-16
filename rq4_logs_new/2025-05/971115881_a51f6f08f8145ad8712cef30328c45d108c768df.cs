using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace TugasBesar_KPL_2425_Kelompok_4.GarbageCollectionSchedule
{
    
    class configPendaftaraanArea
    {
        string configPath = "daftarArea.json";
        public int id { get; set; }
        public string area { get; set; }

        List<configPendaftaraanArea> listArea = new List<configPendaftaraanArea>();
        public void LoadConvig()
        {
            if (File.Exists(configPath))
            {
                string read = File.ReadAllText(configPath);
                if (!string.IsNullOrWhiteSpace(read))
                {
                    listArea = JsonSerializer.Deserialize<List<configPendaftaraanArea>>(read);
                    if (listArea == null)
                    { 
                        listArea = new List<configPendaftaraanArea>();
                    }
                }
            }
        }
        public void saveArea() 
        {
            LoadConvig();

            int maxId = 0;
            foreach (var i in listArea)
            {
                if (i.id > maxId)
                {
                    maxId = i.id;
                }
            }
            this.id = maxId + 1;
            listArea.Add(this);

            string newFile = JsonSerializer.Serialize(listArea, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText("daftarArea.json", newFile);
        }
        
    };

            
}