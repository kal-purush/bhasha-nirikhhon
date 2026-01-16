using System.Collections.Generic;
using System.Linq;

namespace WorldAndroidRadio
{
    public class RadioChannel
    {
        public string Country { get; set; }
        public string Url { get; set; }
        public string Name { get; set; }
    }
    public class ListRadio
    {
        string Country = string.Empty;
        public ListRadio() { }
        public ListRadio(string Country)
        {
            this.Country = Country;
        }
        public List<RadioChannel> GetAllCountry()
        {
            return lstRadio.GroupBy(p => p.Country).Select(grp => grp.FirstOrDefault()).OrderBy(x => x.Country).ToList();
        }
        public List<RadioChannel> GetCountryWiseRadio()
        {
            return lstRadio.Where(p => p.Country == Country).OrderBy(x => x.Name).ToList();
        }

        List<RadioChannel> lstRadio = new List<RadioChannel>()
        {
           new RadioChannel {  Country = "Indonesia", Name = "ANDIKA FM",  Url = "http://stream2.andikafm.com:1057/andikafm" },
           new RadioChannel {  Country = "Indonesia", Name = "ACTARI FM CIAMIS",  Url = "http://stream.suararadio.com:8000/ciamis_actarifm_mp3" },
           new RadioChannel {  Country = "Indonesia", Name = "ADS Radio",  Url = "http://175.103.56.9:8009/" },
           new RadioChannel {  Country = "Indonesia", Name = "Aje Radio",  Url = "http://u.klikhost.com:7854/;stream.mp3" },
           new RadioChannel {  Country = "Indonesia", Name = "Amboi FM",  Url = "http://72.20.10.60:8000" },
           new RadioChannel {  Country = "Indonesia", Name = "An Nabawi Radio",  Url = "http://rosetta.shoutca.st:8313" },
           new RadioChannel {  Country = "Indonesia", Name = "Annabawi FM",  Url = "http://listen.shoutcast.com/annabawiyahfm" },
           new RadioChannel {  Country = "Indonesia", Name = "Ardan Radio",  Url = "http://listento.ardanradio.com:1059" },
           new RadioChannel {  Country = "Indonesia", Name = "ARRIE Radio",  Url = "http://45.64.97.82:8216" },

           new RadioChannel {  Country = "Canada", Name = "SoundFM",  Url = "http://192.240.102.133:7703/stream" },
           new RadioChannel {  Country = "Canada", Name = "Cruz FM",  Url = "https://aispool.streamon.fm/ais/CHFT.aac" },
           new RadioChannel {  Country = "Canada", Name = "Big FM",  Url = "http://live.leanstream.co/CIQBFM" },
           new RadioChannel {  Country = "Canada", Name = "101.3 Kool FM",  Url = "http://newcap.leanstream.co/CJEGFM" },
           new RadioChannel {  Country = "Canada", Name = "101.5 Kool FM",  Url = "http://ais.streamon.fm:8000/ais/CKCE-48k.aac" },
           new RadioChannel {  Country = "Canada", Name = "The GIANT",  Url = "http://newcap.leanstream.co/CHRKFM" },

           new RadioChannel {  Country = "Bangladesh", Name = "Radio Foorti",  Url = "http://119.148.23.88:1021/;stream/1" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Today",  Url = "http://stream.zenolive.com/8wv4d8g4344tv" },
           new RadioChannel {  Country = "Bangladesh", Name = "Dhaka FM",  Url = "http://118.179.219.244:8000" },
           new RadioChannel {  Country = "Bangladesh", Name = "Abc Radio",  Url = "http://stream.zenolive.com/6a28tbx6vewtv" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Bhumi",  Url = "http://noasrv.caster.fm:10182/live" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Shongi",  Url = "http://162.254.150.34:7031" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Sarabela",  Url = "http://162.254.149.146:9308/stream" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Mahananda",  Url = "http://cast.nawabhost.com:8010/stream" },
           new RadioChannel {  Country = "Bangladesh", Name = "Bangla Wadio",  Url = "http://162.254.150.34:8201/" },
           new RadioChannel {  Country = "Bangladesh", Name = "Bangladesh Betar",  Url = "http://192.235.87.105:14322/" },
           new RadioChannel {  Country = "Bangladesh", Name = "Lemon 24",  Url = "http://office.mcc.com.bd:8000" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio 2Fun",  Url = "http://216.107.153.51:8000/airtime_128" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Deshi", Url = "http://37.187.79.93:8016/live" },
           new RadioChannel {  Country = "Bangladesh", Name = "Radio Kotha", Url = "http://188.165.201.120:8016/stream" },
           new RadioChannel {  Country = "Bangladesh", Name = "Jago FM",  Url = "http://192.240.102.133:12110/;stream/1" }
        };
    }
}