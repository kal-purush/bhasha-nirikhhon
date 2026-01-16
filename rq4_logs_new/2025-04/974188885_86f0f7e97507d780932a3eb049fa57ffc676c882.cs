namespace Eds.Shared.Helper.VeribanGlobal.Library.Common.ConstRepository
{
    public class City
    {
        public int Id { get; set; }


        //Şehir Adı
        public string Name { get; set; }

        //Şehir Kodu
        public string CityCode { get; set; }

        //Ülke Id
        //foreign key
        public string CountryCode { get; set; }



        private static List<City> _cities;

        private static object lockSys = new object();

        static City()
        {
            lock (lockSys)
            {
                buildCities();
            }
        }

        public static List<City> GetCityList()
        {
            return _cities;
        }

        /// <summary>
        /// Şehir Listesi
        /// </summary>
        /// <returns></returns>
        private static void buildCities()
        {
            _cities = new List<City>();
            _cities.Add(new City() { CityCode = "-", CountryCode = "-", Name = "Diğer", });
            _cities.Add(new City() { CityCode = "1", CountryCode = "TR", Name = "Adana", });
            _cities.Add(new City() { CityCode = "2", CountryCode = "TR", Name = "Adıyaman", });
            _cities.Add(new City() { CityCode = "3", CountryCode = "TR", Name = "Afyonkarahisar", });
            _cities.Add(new City() { CityCode = "4", CountryCode = "TR", Name = "Ağrı", });
            _cities.Add(new City() { CityCode = "5", CountryCode = "TR", Name = "Amasya", });
            _cities.Add(new City() { CityCode = "6", CountryCode = "TR", Name = "Ankara", });
            _cities.Add(new City() { CityCode = "7", CountryCode = "TR", Name = "Antalya", });
            _cities.Add(new City() { CityCode = "8", CountryCode = "TR", Name = "Artvin", });
            _cities.Add(new City() { CityCode = "9", CountryCode = "TR", Name = "Aydın", });
            _cities.Add(new City() { CityCode = "10", CountryCode = "TR", Name = "Balıkesir", });
            _cities.Add(new City() { CityCode = "11", CountryCode = "TR", Name = "Bilecik", });
            _cities.Add(new City() { CityCode = "12", CountryCode = "TR", Name = "Bingöl", });
            _cities.Add(new City() { CityCode = "13", CountryCode = "TR", Name = "Bitlis", });
            _cities.Add(new City() { CityCode = "14", CountryCode = "TR", Name = "Bolu", });
            _cities.Add(new City() { CityCode = "15", CountryCode = "TR", Name = "Burdur", });
            _cities.Add(new City() { CityCode = "16", CountryCode = "TR", Name = "Bursa", });
            _cities.Add(new City() { CityCode = "17", CountryCode = "TR", Name = "Çanakkale", });
            _cities.Add(new City() { CityCode = "18", CountryCode = "TR", Name = "Çankırı", });
            _cities.Add(new City() { CityCode = "19", CountryCode = "TR", Name = "Çorum", });
            _cities.Add(new City() { CityCode = "20", CountryCode = "TR", Name = "Denizli", });
            _cities.Add(new City() { CityCode = "21", CountryCode = "TR", Name = "Diyarbakır", });
            _cities.Add(new City() { CityCode = "22", CountryCode = "TR", Name = "Edirne", });
            _cities.Add(new City() { CityCode = "23", CountryCode = "TR", Name = "Elazığ", });
            _cities.Add(new City() { CityCode = "24", CountryCode = "TR", Name = "Erzincan", });
            _cities.Add(new City() { CityCode = "25", CountryCode = "TR", Name = "Erzurum", });
            _cities.Add(new City() { CityCode = "26", CountryCode = "TR", Name = "Eskişehir", });
            _cities.Add(new City() { CityCode = "27", CountryCode = "TR", Name = "Gaziantep", });
            _cities.Add(new City() { CityCode = "28", CountryCode = "TR", Name = "Giresun", });
            _cities.Add(new City() { CityCode = "29", CountryCode = "TR", Name = "Gümüşhane", });
            _cities.Add(new City() { CityCode = "30", CountryCode = "TR", Name = "Hakkari", });
            _cities.Add(new City() { CityCode = "31", CountryCode = "TR", Name = "Hatay", });
            _cities.Add(new City() { CityCode = "32", CountryCode = "TR", Name = "Isparta", });
            _cities.Add(new City() { CityCode = "33", CountryCode = "TR", Name = "Mersin", });
            _cities.Add(new City() { CityCode = "34", CountryCode = "TR", Name = "İstanbul", });
            _cities.Add(new City() { CityCode = "35", CountryCode = "TR", Name = "İzmir", });
            _cities.Add(new City() { CityCode = "36", CountryCode = "TR", Name = "Kars", });
            _cities.Add(new City() { CityCode = "37", CountryCode = "TR", Name = "Kastamonu", });
            _cities.Add(new City() { CityCode = "38", CountryCode = "TR", Name = "Kayseri", });
            _cities.Add(new City() { CityCode = "39", CountryCode = "TR", Name = "Kırklareli", });
            _cities.Add(new City() { CityCode = "40", CountryCode = "TR", Name = "Kırşehir", });
            _cities.Add(new City() { CityCode = "41", CountryCode = "TR", Name = "Kocaeli", });
            _cities.Add(new City() { CityCode = "42", CountryCode = "TR", Name = "Kahramanmaraş", });
            _cities.Add(new City() { CityCode = "43", CountryCode = "TR", Name = "Konya", });
            _cities.Add(new City() { CityCode = "44", CountryCode = "TR", Name = "Kütahya", });
            _cities.Add(new City() { CityCode = "45", CountryCode = "TR", Name = "Malatya", });
            _cities.Add(new City() { CityCode = "46", CountryCode = "TR", Name = "Manisa", });
            _cities.Add(new City() { CityCode = "47", CountryCode = "TR", Name = "Mardin", });
            _cities.Add(new City() { CityCode = "48", CountryCode = "TR", Name = "Muğla", });
            _cities.Add(new City() { CityCode = "49", CountryCode = "TR", Name = "Muş", });
            _cities.Add(new City() { CityCode = "50", CountryCode = "TR", Name = "Nevşehir", });
            _cities.Add(new City() { CityCode = "51", CountryCode = "TR", Name = "Niğde", });
            _cities.Add(new City() { CityCode = "52", CountryCode = "TR", Name = "Ordu", });
            _cities.Add(new City() { CityCode = "53", CountryCode = "TR", Name = "Rize", });
            _cities.Add(new City() { CityCode = "54", CountryCode = "TR", Name = "Sakarya", });
            _cities.Add(new City() { CityCode = "55", CountryCode = "TR", Name = "Samsun", });
            _cities.Add(new City() { CityCode = "56", CountryCode = "TR", Name = "Siirt", });
            _cities.Add(new City() { CityCode = "57", CountryCode = "TR", Name = "Sinop", });
            _cities.Add(new City() { CityCode = "58", CountryCode = "TR", Name = "Sivas", });
            _cities.Add(new City() { CityCode = "59", CountryCode = "TR", Name = "Tekirdağ", });
            _cities.Add(new City() { CityCode = "60", CountryCode = "TR", Name = "Tokat", });
            _cities.Add(new City() { CityCode = "61", CountryCode = "TR", Name = "Trabzon", });
            _cities.Add(new City() { CityCode = "62", CountryCode = "TR", Name = "Tunceli", });
            _cities.Add(new City() { CityCode = "63", CountryCode = "TR", Name = "Şanlıurfa", });
            _cities.Add(new City() { CityCode = "64", CountryCode = "TR", Name = "Uşak", });
            _cities.Add(new City() { CityCode = "65", CountryCode = "TR", Name = "Van", });
            _cities.Add(new City() { CityCode = "66", CountryCode = "TR", Name = "Yozgat", });
            _cities.Add(new City() { CityCode = "67", CountryCode = "TR", Name = "Zonguldak", });
            _cities.Add(new City() { CityCode = "68", CountryCode = "TR", Name = "Aksaray", });
            _cities.Add(new City() { CityCode = "69", CountryCode = "TR", Name = "Bayburt", });
            _cities.Add(new City() { CityCode = "70", CountryCode = "TR", Name = "Karaman", });
            _cities.Add(new City() { CityCode = "71", CountryCode = "TR", Name = "Kırıkkale", });
            _cities.Add(new City() { CityCode = "72", CountryCode = "TR", Name = "Batman", });
            _cities.Add(new City() { CityCode = "73", CountryCode = "TR", Name = "Şırnak", });
            _cities.Add(new City() { CityCode = "74", CountryCode = "TR", Name = "Bartın", });
            _cities.Add(new City() { CityCode = "75", CountryCode = "TR", Name = "Ardahan", });
            _cities.Add(new City() { CityCode = "76", CountryCode = "TR", Name = "Iğdır", });
            _cities.Add(new City() { CityCode = "77", CountryCode = "TR", Name = "Yalova", });
            _cities.Add(new City() { CityCode = "78", CountryCode = "TR", Name = "Karabük", });
            _cities.Add(new City() { CityCode = "79", CountryCode = "TR", Name = "Kilis", });
            _cities.Add(new City() { CityCode = "80", CountryCode = "TR", Name = "Osmaniye", });
            _cities.Add(new City() { CityCode = "81", CountryCode = "TR", Name = "Düzce", });
        }
    }
}