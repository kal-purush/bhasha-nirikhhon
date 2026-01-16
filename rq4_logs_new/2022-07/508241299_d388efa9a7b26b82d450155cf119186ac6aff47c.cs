using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Web;

namespace Function
{
    public class AliceRequestBase
    {


        [JsonPropertyName("meta")]
        public AliceMetaModel Meta { get; set; }

        [JsonPropertyName("session")]
        public AliceSessionModel Session { get; set; }

        [JsonPropertyName("request")]
        public AliceRequestModel Request { get; set; }

        [JsonPropertyName("version")]
        public string Version { get; set; }
    }
    public class AliceMetaModel
    {
        [JsonPropertyName("locale")]
        public string Locale { get; set; }

        [JsonPropertyName("timezone")]
        public string Timezone { get; set; }

        [JsonPropertyName("client_id")]
        public string ClientId { get; set; }

        [JsonPropertyName("interfaces")]
        public AliceInterfacesModel Interfaces { get; set; }
    }

    public class AliceInterfacesModel
    {
        [JsonPropertyName("screen")]
        public object Screen { get; set; }

        [JsonPropertyName("payments")]
        public object Payments { get; set; }

        [JsonPropertyName("account_linking")]
        public object AccountLinking { get; set; }


    }

    public class AliceSessionModel
    {
        [JsonPropertyName("new")]
        public bool New { get; set; }

        [JsonPropertyName("session_id")]
        public string SessionId { get; set; }

        [JsonPropertyName("message_id")]
        public int MessageId { get; set; }

        [JsonPropertyName("skill_id")]
        public string SkillId { get; set; }

        [JsonPropertyName("user_id")]
        public string UserId { get; set; }

        [JsonPropertyName("user")]
        public AliceSessionUserModel User { get; set; }

        [JsonPropertyName("application")]
        public AliceSessionApplicationModel Application { get; set; }


    }
    public class AliceSessionUserModel
    {
        [JsonPropertyName("user_id")]
        public string UserId { get; set; }

        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; }
    }
    public class AliceSessionApplicationModel
    {
        [JsonPropertyName("application_id")]
        public string ApplicationId { get; set; }
    }

    public class AliceRequestModel
    {
        [JsonPropertyName("command")]
        public string Command { get; set; }

        [JsonPropertyName("original_utterance")]
        public string OriginalUtterance { get; set; }

        [JsonPropertyName("payload")]
        public object Payload { get; set; }

        [JsonPropertyName("markup")]
        public AliceMarkupModel Markup { get; set; }

        [JsonPropertyName("type")]
        public string Type { get; set; }

        //nlu
        [JsonPropertyName("nlu")]
        public AliceNluModel Nlu { get; set; }
    }
    #region NLU
    public class AliceMarkupModel
    {
        [JsonPropertyName("dangerous_context")]
        public bool DangerousContext { get; set; }
    }
    //nlu
    public class AliceNluModel
    {
        [JsonPropertyName("tokens")]
        public IEnumerable<string> Tokens { get; set; }

        [JsonPropertyName("entities")]
        [JsonConverter(typeof(AliceEntityModelEnumerableConverter))]
        public IEnumerable<AliceEntityModel> Entities { get; set; }



        [JsonPropertyName("intents")]
        public object Intents { get; set; }
    }
    #region nluModels
    public class AliceEntityNumberModel : AliceEntityModel
    {
        [JsonPropertyName("value")]
        public double Value { get; set; }
    }
    public class AliceEntityGeoModel : AliceEntityModel
    {
        [JsonPropertyName("value")]
        public AliceEntityGeoValueModel Value { get; set; }
    }
    public class AliceEntityGeoValueModel
    {
        [JsonPropertyName("house_number")]
        public string HouseNumber { get; set; }

        [JsonPropertyName("street")]
        public string Street { get; set; }

        [JsonPropertyName("city")]
        public string City { get; set; }

        [JsonPropertyName("country")]
        public string Country { get; set; }

        [JsonPropertyName("airport ")]
        public string Airport { get; set; }
    }
    public class AliceEntityFioModel : AliceEntityModel
    {
        [JsonPropertyName("value")]
        public AliceEntityFioValueModel Value { get; set; }
    }
    public class AliceEntityFioValueModel
    {
        [JsonPropertyName("first_name")]
        public string FirstName { get; set; }

        [JsonPropertyName("last_name")]
        public string LastName { get; set; }
        [JsonPropertyName("patronymic_name")]
        public string PatronymicName { get; set; }
    }
    public class AliceEntityStringModel : AliceEntityModel
    {
        [JsonPropertyName("value")]
        public string Value { get; set; }
    }
    public class AliceEntityDateTimeModel : AliceEntityModel
    {
        [JsonPropertyName("value")]
        public AliceEntityDateTimeValueModel Value { get; set; }
    }
    public class AliceEntityDateTimeValueModel
    {
        [JsonPropertyName("day")]
        public double Day { get; set; }

        [JsonPropertyName("day_is_relative")]
        public bool DayIsRelative { get; set; }

        [JsonPropertyName("hour")]
        public double Hour { get; set; }

        [JsonPropertyName("hour_is_relative")]
        public bool HourIsRelative { get; set; }

        [JsonPropertyName("minute")]
        public double Minute { get; set; }

        [JsonPropertyName("minute_is_relative")]
        public bool MinuteIsRelative { get; set; }

        [JsonPropertyName("month")]
        public double Month { get; set; }

        [JsonPropertyName("month_is_relative")]
        public bool MonthIsRelative { get; set; }

        [JsonPropertyName("year")]
        public double Year { get; set; }

        [JsonPropertyName("year_is_relative")]
        public bool YearIsRelative { get; set; }
    }
    #endregion
    public class AliceEntityModelEnumerableConverter : EnumerableConverter<AliceEntityModel>
    {
        private static readonly Dictionary<string, Type> _typeMap = new Dictionary<string, Type>
        {
            { "YANDEX.GEO", typeof(AliceEntityGeoModel) },
            { "YANDEX.FIO", typeof(AliceEntityFioModel) },
            { "YANDEX.NUMBER", typeof(AliceEntityNumberModel) },
            { "YANDEX.DATETIME", typeof(AliceEntityDateTimeModel) },
            { "YANDEX.STRING", typeof(AliceEntityStringModel) },
        };
        protected override AliceEntityModel ToItem(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return null;
            }

            var readerAtStart = reader;

            string type = null;
            using (var jsonDocument = JsonDocument.ParseValue(ref reader))
            {
                var jsonObject = jsonDocument.RootElement;

                type = jsonObject
                    .EnumerateObject()
                    .FirstOrDefault(x => x.Name == "type")
                    .Value.GetString();
            }
            if (!string.IsNullOrEmpty(type) && _typeMap.TryGetValue(type, out var targetType))
            {
                return JsonSerializer.Deserialize(ref readerAtStart, targetType, options) as AliceEntityModel;
            }
            return null;
        }

        protected override void WriteItem(Utf8JsonWriter writer, AliceEntityModel item, JsonSerializerOptions options)
        {
            object newValue = null;
            if (item != null)
            {
                newValue = Convert.ChangeType(item, item.GetType(), CultureInfo.InvariantCulture);
            }

            JsonSerializer.Serialize(writer, newValue, options);
        }
    }
    public abstract class EnumerableConverter<TItem> : JsonConverter<IEnumerable<TItem>>
    {
        public override IEnumerable<TItem> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartArray:
                    var list = new List<TItem>();
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            break;
                        }

                        list.Add(ToItem(ref reader, options));
                    }

                    return list.ToArray();
                case JsonTokenType.None:
                case JsonTokenType.StartObject:
                case JsonTokenType.EndObject:
                case JsonTokenType.EndArray:
                case JsonTokenType.PropertyName:
                case JsonTokenType.Comment:
                case JsonTokenType.String:
                case JsonTokenType.Number:
                case JsonTokenType.True:
                case JsonTokenType.False:
                case JsonTokenType.Null:
                default:
                    return Array.Empty<TItem>();
            }
        }

        public override void Write(Utf8JsonWriter writer, IEnumerable<TItem> value, JsonSerializerOptions options)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            if (value == null)
            {
                return;
            }

            writer.WriteStartArray();
            foreach (var item in value)
            {
                WriteItem(writer, item, options);
            }

            writer.WriteEndArray();
        }

        protected abstract TItem ToItem(ref Utf8JsonReader reader, JsonSerializerOptions options);

        protected virtual void WriteItem(Utf8JsonWriter writer, TItem item, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, item, options);
        }
    }
    public class AliceEntityModel
    {
        [JsonPropertyName("tokens")]
        public NluTokens Tokens { get; set; }

        [JsonPropertyName("type")]
        public string Type { get; set; }
    }



    public class NluTokens
    {
        [JsonPropertyName("start")]
        public int start { get; set; }
        [JsonPropertyName("end")]
        public int end { get; set; }

    }



    #endregion
    #region audio
    public class AliceAudioResponse : IAliceResponse
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("response")]
        public AliceAudioResponseModel Response { get; set; }

        public AliceAudioResponse()
        {
            Response = new AliceAudioResponseModel();
            Version = "1.0";
        }
    }

    public class AliceAudioResponseModel
    {
        public AliceAudioResponseModel()
        {
            ShowItemMeta = new ShowMeta();       
                 ShowItemMeta.publication_date = DateTime.UtcNow.ToString("o");
            ShowItemMeta.content_id="1";
            ShowItemMeta.id="1";

            Directives = new AliceDirectives();
        }
        [JsonPropertyName("text")]
        public string Text { get; set; }
        [JsonPropertyName("tts")]
        public string tts { get; set; }

        [JsonPropertyName("end_session")]
        public bool EndSession { get; set; }

        [JsonPropertyName("should_listen")]
        public bool ShouldListen { get; set; } = false;

        [JsonPropertyName("show_item_meta")]
        public ShowMeta ShowItemMeta { get; set; }

        [JsonPropertyName("directives")]
        public AliceDirectives Directives { get; set; }
    }
    public class AliceDirectives
    {
        [JsonPropertyName("audio_player")] 
        public AudioPlayerDirective audioPlayer { get; set; }

        public AliceDirectives()
        {
            audioPlayer = new AudioPlayerDirective();
        }
    }
    public class AudioPlayerDirective
    {
        [JsonPropertyName("action")] 
        public string Action { get; set; }="Play";
        [JsonPropertyName("item")]
        public AudioItem item { get; set; }

        [JsonPropertyName("type")]

        public string type { get;set;}="audio_player";
        
        public AudioPlayerDirective()
        {
            item = new AudioItem();
        }
    }
    public class AudioItem
    {
        [JsonPropertyName("stream")] 
        public AudioStream stream { get; set; }

        [JsonPropertyName("metadata")]
        public AudioMetadata metadata { get; set; }

        public AudioItem()
        {
            stream = new AudioStream();
            stream.token = Guid.NewGuid().ToString();
            metadata = new AudioMetadata();
            metadata.SongArtpic = new urlclass();
            metadata.SongBgimg = new urlclass();
        }
    }
    public class AudioStream
    {
        [JsonPropertyName("url")] 
        public string url { get; set; }

        [JsonPropertyName("token")] 
        public string token { get; set; }

        [JsonPropertyName("offset_ms")]
        public int offsetms { get; set; } = 0;

    }

    public class AudioMetadata
    {
        [JsonPropertyName("title")]
        public string SongTitle { get; set; }
        [JsonPropertyName("sub_title")] 
        public string SongArtist { get; set; }

        [JsonPropertyName("art")]
        public urlclass SongArtpic { get; set; }
        [JsonPropertyName("background_image")]
        public urlclass SongBgimg { get; set; }
    }
public class urlclass {
            [JsonPropertyName("url")]

    public string url {get;set;}="https://ya.ru";
}
    #endregion
    
    public class AliceResponseModel
    {
        private string _tts;
        private const int _textMaxLength = 1024;
        private const int _ttsMaxLength = 1024;

        private string _text;

        [JsonPropertyName("text")]
        public string Text
        {
            get => _text;

            set
            {
                _text = value;
            }
        }
        [JsonPropertyName("tts")]
        public string Tts
        {
            get => _tts;

            set
            {
                _tts = value;
            }
        }

        [JsonPropertyName("end_session")]
        public bool EndSession { get; set; }

        [JsonPropertyName("show_item_meta")]
        public ShowMeta ShowItemMeta { get; set; }
        [JsonPropertyName("buttons")]
        public List<ButtonResponse> buttons {get;set;}
    }
    public class ShowMeta
    {
        [JsonPropertyName("publication_date")]
        public string publication_date { get; set; }
        [JsonPropertyName("id")]
        public string id { get; set; }
        [JsonPropertyName("content_id")]
        public string content_id { get; set; }
    }
    public static class Storage
    {
        public static string Auth = "";
        public static string Code = "";
    }
    #region LOGIC
    public class AliceResponse : IAliceResponse
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("response")]
        public AliceResponseModel Response { get; set; }
        public AliceResponse()
        {
            Version = "1.0";
            Response = new AliceResponseModel();
        }
        public AliceResponse(AliceRequestBase request)
        {
            string funcurl = "https://functions.yandexcloud.net/d4entjkagd3fqn0hum1d";
            Version = request.Version;
            Response = new AliceResponseModel();
            Response.EndSession = false;
            var sh = new ShowMeta();
            //some random guid
            sh.content_id = "88f83f54-1135-4238-85c4-5e45959a64d0";
            sh.id = "88f83f54-1135-4238-85c4-5e45959a64d0";
            sh.publication_date = DateTime.UtcNow.ToString("o");
            string reqC = request.Request.Command;
            string originalU = request.Request.OriginalUtterance;
            if (originalU == "команда001")
            {
                reqC = reqC + " можно обработать команду как-то ";
            }

            if (reqC == "привет")
            {
                reqC = reqC + " медвед ";
            }

            string help = " вот что я умею: скажи чегототам и я сделаю что-нибудь.";
            Response.ShowItemMeta = sh;
            if (reqC == string.Empty ||
                reqC.ToLower() == "помощь" ||
                reqC.ToLower().Contains("что ты умеешь"))
            {

                Response.Text = "Привет! " + help;
            }
            else if (reqC.ToLower().Contains("авторизация"))
            {
                string auth = "https://oauth.yandex.ru/authorize?response_type=code&client_id=e738f647b8b145948b7a5a622a59819d&redirect_uri="
                    + funcurl;

                Response.Text = "попробуй эту ссылку " + auth;

            }
            else if (reqC.ToLower().Contains("проверка"))
            {
                Response.Text = "code=" + Storage.Code + " токен =" + Storage.Auth;
            }
            else if (reqC.ToLower().Contains("проверка"))
            {

            }
            else
            {
                //извлечение чисел из входящего запроса
                var ints = request.Request.Nlu.Entities.Where(t => t.Type == "YANDEX.NUMBER").ToList();
                string sints = "";
                if (ints != null && ints.Count > 0)
                {
                    sints = ". А еще вот числа из запроса: ";
                    foreach (var item in ints)
                    {
                        if (item != null)

                            sints = sints + " " + (item as AliceEntityNumberModel).Value;


                    }

                }
                //попугай

                Response.Text = "ПРИВЕТ ПОВЕЛИТЕЛЬ! вот что прислали: " + reqC + sints;
                Response.Tts = Response.Text;
            }

        }
    }
    public interface IAliceResponse
    { }
    #region Cards
    public class Card
    {
        [JsonPropertyName("type")]
        public string type {get;set;}="BigImage";

        [JsonPropertyName("image_id")]
        public string imageid {get;set;}

        [JsonPropertyName("title")]
        public string title {get;set;}

        [JsonPropertyName("description")]
        public string description {get;set;}
        
        [JsonPropertyName("button")]
        public ButtonCard button {get;set;}

        public Card()
        {
            //type = "BigImage";
            //imageid = "937455/d8bdb0e65cce413fcfd8";
            //title = "anonimous";
            //description = "anonimous";
            //button = new ButtonCard();
            //button.text = "команда001";
        }
        
    }
    public class Header
    {
        [JsonPropertyName("text")]
        public string Text {get;set;}

    }
    public class ItemsListCard
    {
        [JsonPropertyName("type")]
        public string type {get;set;}="ItemsList";

        

        [JsonPropertyName("header")]
        public Header header {get;set;}
        [JsonPropertyName("items")]
        public List<Card> cards {get;set;}
        
        
        [JsonPropertyName("footer")]
        public object footer {get;set;}
        public ItemsListCard()
        {
            cards = new List<Card>();
            
            for (int i=0;i<3;i++)
            {
                Card tek = new Card();
                tek.type = "BigImage";
                
                if (i==0)
                    {
                        tek.imageid = "937455/d8bdb0e65cce413fcfd8";
                        tek.title = "anonimous";
                        tek.description = "anonimous";
                    }
                else if (i==1) 
                {
                    tek.imageid = "1652229/c8ed0dce3a75694da643";
                    tek.title = "rybakov";
                    tek.description = "rybakoff";
                }
                else if (i==2) 
                {
                    tek.imageid = "1652229/d39fb0311b5bf97fe508";
                    tek.title = "vesna";
                    tek.description = "vesnaaaa";
                }

               
                
                tek.button = new ButtonCard();
                tek.button.text = "команда00"+i.ToString();
                cards.Add(tek);
            }
        }
    }
    public class ButtonCard
    {
        [JsonPropertyName("text")]
        public string text {get;set;}

        [JsonPropertyName("url")]
        public string url {get;set;}
        
        [JsonPropertyName("payload")]
        public object payload {get;set;}
    }
    public class ButtonResponse
    {
        [JsonPropertyName("title")]
        public string title {get;set;}

        [JsonPropertyName("url")]
        public string url {get;set;}

        [JsonPropertyName("hide")]
        public bool hide { get; set; } = false;

        [JsonPropertyName("payload")]
        public object payload {get;set;}
    }
    public class AliceResponseCard : IAliceResponse
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("response")]
        public AliceResponseModelCard Response { get; set; }
        public AliceResponseCard()
        {
            Version = "1.0";
            Response = new AliceResponseModelCard();
        }
        
    }
    public class AliceResponseCardList : IAliceResponse
    {
        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("response")]
        public AliceResponseModelCardList Response { get; set; }
        public AliceResponseCardList()
        {
            Version = "1.0";
            Response = new AliceResponseModelCardList();
        }
        
    }
    public class AliceResponseModelCardList
    {
        private string _tts;
        private const int _textMaxLength = 1024;
        private const int _ttsMaxLength = 1024;

        private string _text;

        [JsonPropertyName("text")]
        public string Text
        {
            get => _text;

            set
            {
                _text = value;
            }
        }
        [JsonPropertyName("tts")]
        public string Tts
        {
            get => _tts;

            set
            {
                _tts = value;
            }
        }

        [JsonPropertyName("end_session")]
        public bool EndSession { get; set; }

        [JsonPropertyName("show_item_meta")]
        public ShowMeta ShowItemMeta { get; set; }
        [JsonPropertyName("card")]
        public ItemsListCard card {get;set;}
        [JsonPropertyName("buttons")]
        public List<ButtonResponse> buttons {get;set;}
    }
    public class AliceResponseModelCard
    {
        private string _tts;
        private const int _textMaxLength = 1024;
        private const int _ttsMaxLength = 1024;

        private string _text;

        [JsonPropertyName("text")]
        public string Text
        {
            get => _text;

            set
            {
                _text = value;
            }
        }
        [JsonPropertyName("tts")]
        public string Tts
        {
            get => _tts;

            set
            {
                _tts = value;
            }
        }

        [JsonPropertyName("end_session")]
        public bool EndSession { get; set; }

        [JsonPropertyName("show_item_meta")]
        public ShowMeta ShowItemMeta { get; set; }
        [JsonPropertyName("card")]
        public Card card {get;set;}
        [JsonPropertyName("buttons")]
        public List<ButtonResponse> buttons { get; set; }
    }
    #endregion

    #endregion
    #region Pictures
    public class Pics
    {
        
        public Dictionary<string,string> picAns = new Dictionary<string, string>();

        public List<string> words = new List<string>();
        public Pics()
        {
            //массив ИД картинок и верных ответов
            picAns.Add("1030494/d07f2394c26fe3fb114b","лунула");
            picAns.Add("965417/4afc4a742c36078569b8","лемниската");
            picAns.Add("1521359/e7d50ead677ce37f07ae","феррул");
            picAns.Add("1533899/0c26a5bd155a96a77923", "папер");
            picAns.Add("937455/0a90e9df7ca15de2d34b", "глабелла");
            
            //массив остальных умных слов
            words.Add("нердл");
            words.Add("зарф");
            words.Add("титл");
            words.Add("пучки флоэмы");
            words.Add("эглет");
            words.Add("папер");
            words.Add("парестезия");
            words.Add("пантограф");
            words.Add("гастралгия");
            words.Add("вагитус");
            words.Add("глабелла");
            words.Add("натиформа");
            words.Add("распялка");
            words.Add("лунула");
            words.Add("керлер");
            words.Add("минимус");
            words.Add("нефофобия");
            words.Add("малая пядь");
            words.Add("лемниската");
            words.Add("феррул");
        }
        public PicForGame generatePic()
        {
            PicForGame pic = new PicForGame();
            Random rnd = new Random();
            //выбираем картинку из массива случайно
            int indPic = rnd.Next(0, picAns.Count);
            //ид картинки и правильный ответ
            string picid = picAns.ElementAt(indPic).Key;
            string Ans = picAns.ElementAt(indPic).Value;
            pic.picId = picid;
            pic.AnsR = Ans;

            //выбираем остальные ответы из массива слов
            var l1 = words.Where(w=> w!=Ans ).ToList();
            int ind1 = rnd.Next(0, l1.Count);
            string ans1 = l1[ind1];

            var l2 = words.Where( w=> w!=Ans && w!=ans1 ).ToList();
            int ind2 = rnd.Next(0, l2.Count);
            string ans2 = l2[ind2];

            var l3 = words.Where(w=> w!=Ans  && w!=ans1  && w!=ans2 ).ToList();
            int ind3 = rnd.Next(0, l3.Count);
            string ans3 = l3[ind3];
            //добавляем ответы в класс картинки
            pic.Ans2 = ans1;
            pic.Ans3 = ans2;
            pic.Ans4 = ans3;
            //добавляем в массив
            pic.Answers4.Add(Ans);
            pic.Answers4.Add(ans1);
            pic.Answers4.Add(ans2);
            pic.Answers4.Add(ans3);
            //перемешиваем массив
            pic.Answers4 = pic.Answers4.OrderBy(x=>rnd.Next()).ToList();

            return pic; 
        }
    }
    public class PicForGame
    {
        public string picId="";
        public string AnsR="";

        public string Ans2="";
        public string Ans3="";
        public string Ans4="";

        public List<string> Answers4 = new List<string>();

    }
    public static class StorageU
    {
        
        public async static Task<string> WriteFile()
        {
            //запрос к БД на сохранение данных
            //storage.yandexcloud.net
            //http put
            string url = "https://verticalvideoportal.ru/file.php";
            
            url = url + "?regim=" + regimUgadaika.ToString() +
                "&ugadal=" + ugadal.ToString() +
                "&neugadal=" + neugadal.ToString() +
                "&rightanswer=" + RightAnswer.ToString();

            using (var client = new HttpClient())
            {
                var urli = new Uri(url);
                client.BaseAddress = new Uri(url);
                var response = await client.GetAsync(urli);
                //правильно сделать GET запрос с параметрами 
            }
            return "ok";

        }
        public async static Task<string> ReadFile()
        {
            //запрос к БД на чтение данных
            //storage.yandexcloud.net
            //делаем http get

            string url = "https://verticalvideoportal.ru/dialog.txt";
            using (var client = new HttpClient())
            {
                var urli = new Uri(url);
                client.BaseAddress = new Uri(url);
                var response = await client.GetAsync(urli);
                string stringbody = await response.Content.ReadAsStringAsync();
                var split = stringbody.Split(";");
                regimUgadaika = Convert.ToBoolean(split[0]);
                ugadal = Convert.ToInt32(split[1]);
                neugadal = Convert.ToInt32(split[2]);
                RightAnswer = split[3];

            }
            return "ok";
        }
        public static bool regimUgadaika = false;
        public static string RightAnswer = "";
        public static int ugadal = 0;
        public static int neugadal = 0;
    }
    #endregion
    public class Handler
    {
        //правильный ответ на угадайку
        
        public async Task<IAliceResponse> FunctionHandler(string requestJ)
        {
            

            if (requestJ.Contains("version") == false)
            {
                if (requestJ.Contains("queryStringParameters"))
                {
                    int startQ = requestJ.IndexOf("queryStringParameters");
                    int start2 = requestJ.IndexOf("code", startQ);
                    int startCode = requestJ.IndexOf(":", start2);
                    int endCode = requestJ.IndexOf("}", start2);
                    string code = requestJ.Substring(startCode, endCode - startCode);
                    code = code.Replace("'", "");
                    code = code.Replace(":", "");
                    code = code.Replace("}", "");
                    code = code.Replace("u0022", "");
                    code = code.Replace("\\", "");
                    code = code.Replace("\'", "");
                    code = code.Replace("\"", "");
                    Storage.Code = code;
                    //int icode = Convert.ToInt32(code);
                    //Приложение отправляет код, а также свой идентификатор и пароль в POST-запросе.
                    HttpClient httpClient = new HttpClient();
                    httpClient.BaseAddress = new Uri("https://oauth.yandex.ru");

                    Dictionary<string, string> headers = new Dictionary<string, string>();
                    headers.Add("grant_type", "authorization_code");
                    headers.Add("code", code);
                    //e738f647b8b145948b7a5a622a59819d
                    //95992604fc3748659bb019212c3f30de
                    headers.Add("client_id", "23cabbbdc6cd418abb4b39c32c41195d");
                    headers.Add("client_secret", "53bc75238f0c4d08a118e51fe9203300");


                    var httpcontent = new FormUrlEncodedContent(headers);

                    var webRequest = new HttpRequestMessage(HttpMethod.Post, "https://oauth.yandex.ru/token")
                    {
                        Content = httpcontent
                    };

                    var res = await httpClient.PostAsync("https://oauth.yandex.ru/token",httpcontent);

                    var jsonBody = res.Content.ReadAsStringAsync().Result;
                    int st = jsonBody.IndexOf("access_token");
                    int st2 = jsonBody.IndexOf(":",st);
                    int st3 = jsonBody.IndexOf(",", st2);
                    string auf = jsonBody.Substring(st2, st3 - st2);
                    auf = auf.Replace(":", "");
                    auf = auf.Replace("\"", "");
                    auf = auf.Trim();
                    Storage.Auth = auf;
                    return new AliceResponse();
                    //throw new Exception("auf=" + auf);
                }
                else
                    //hmmm
                    throw new Exception("req=" + requestJ);

                
            }
            else
            {
                //каждый раз когда приходит запрос
                //обновляем настройки угадайки из файла!
                StorageU.ReadFile();

                AliceRequestBase request = JsonSerializer.Deserialize<AliceRequestBase>(requestJ);
                if (request.Request.Command.ToLower().Contains("аудио"))
                {
                    var audio = new AliceAudioResponse();
                    audio.Response.Text = "послушай эту музыку";
                    audio.Response.tts = "послушай эту музыку";
                    //from api.Track.GetFileLink
                    audio.Response.Directives.audioPlayer.item.stream.url = "https://s53sas.storage.yandex.net/get-mp3/1659b4f005c147acbd5a601d8d0a4c5855992b59/0005e2c05c65f84d/rmusic/U2FsdGVkX1_GK5XMhybGzFpjubChT1YEso7Ai2Fl1iepxICrrCW0XvlkpeB9mwjXmkHCnwrQnLojkHnidVcc7nfbQcWw7hbuPHDNKdaIXso/6e384b312cdfe5fc7cbc23f094a9be3ca04ebac6d25bae22ab1d9debaed51094/31487";
                    audio.Response.Directives.audioPlayer.item.metadata.SongTitle = "question";
                    audio.Response.Directives.audioPlayer.item.metadata.SongArtist = "System of a down";
                    return audio;
                }
                else if (request.Request.Command.ToLower().Contains("картинка"))
                {
                    AliceResponseCard resp = new AliceResponseCard();
                    resp.Response.Text = "карта";
                    resp.Response.Tts = "карта";
                    resp.Response.card = new Card();
                    return resp;
                }
                else if (StorageU.regimUgadaika == true)
                {
                    if (request.Request.OriginalUtterance.ToLower().Contains(StorageU.RightAnswer.ToLower()))
                    {
                        StorageU.ugadal++;
                        AliceResponseCard resp = new AliceResponseCard();
                        resp.Response.Text = "ты угадал, молодец! угадал "+ StorageU.ugadal + " раз";
                        resp.Response.Tts = "ты угадал, молодец!";
                        //выключаем режим угадайки
                        StorageU.regimUgadaika = false;
                        StorageU.RightAnswer = "";
                        StorageU.WriteFile();
                        return resp;
                    }
                    else
                    {
                        StorageU.neugadal++;
                        AliceResponseCard resp = new AliceResponseCard();
                        resp.Response.Text = "ты НЕ угадал, правильный ответ был "+ StorageU.RightAnswer + " НЕ угадал " + StorageU.neugadal + " " + "раз";
                        resp.Response.Tts = "ты НЕ угадал, правильный ответ был " + StorageU.RightAnswer;
                        //выключаем режим угадайки
                        StorageU.regimUgadaika = false;
                        StorageU.RightAnswer = "";
                        StorageU.WriteFile();
                        return resp;
                    }
                    
                }
                //сам режим угадайки
                else if (request.Request.Command.ToLower().Contains("угадайка"))
                {
                    try
                    {
                        StorageU.regimUgadaika = true;
                        Pics pics = new Pics();
                        //генерируем 1 из 3 наших картинок
                        PicForGame pg = pics.generatePic();
                        
                        AliceResponseCard resp = new AliceResponseCard();
                        //заполняем поля ответа
                        resp.Response.Text = "угадай название предмета на картинке";
                        resp.Response.Tts = "угадай название предмета на картинке";
                        var cardG = new Card();
                        //добвляем картинку
                        cardG.type = "BigImage";
                        cardG.imageid = pg.picId;
                        cardG.title = "угадай название предмета на картинке (режим="
                            + StorageU.regimUgadaika.ToString()+")";
                        cardG.description = "угадай название предмета на картинке";
                        //еще раз рандомизируем кнопки
                        Random rnd = new Random();
                        pg.Answers4 = pg.Answers4.OrderBy(x => rnd.Next()).ToList();
                        resp.Response.card = cardG;
                        //добавляем 4 кнопки
                        ButtonResponse btn1 = new ButtonResponse();
                        btn1.title = pg.Answers4[0];
                        ButtonResponse btn2 = new ButtonResponse();
                        btn2.title = pg.Answers4[1];
                        ButtonResponse btn3 = new ButtonResponse();
                        btn3.title = pg.Answers4[2];
                        ButtonResponse btn4 = new ButtonResponse();
                        btn4.title = pg.Answers4[3];

                        resp.Response.buttons = new List<ButtonResponse>();

                        resp.Response.buttons.Add(btn1);
                        resp.Response.buttons.Add(btn2);
                        resp.Response.buttons.Add(btn3);
                        resp.Response.buttons.Add(btn4);

                        //запоминаем правильный ответ
                        StorageU.RightAnswer = pg.AnsR;
                        

                        StorageU.WriteFile();
                        return resp;
                    }
                    catch (Exception ex)
                    {
                        var response = new AliceResponse();
                        response.Response.Text = ex.Message + " -- " + ex.StackTrace;
                        return response;
                    }
                }
                else if (request.Request.Command.ToLower().Contains("коллекция"))
                {
                    AliceResponseCardList resp = new AliceResponseCardList();
                    resp.Response.Text = "карта";
                    resp.Response.Tts = "карта";
                    resp.Response.card = new ItemsListCard();

                    return resp;
                }
                else if (request.Request.Command.ToLower().Contains("кнопки"))
                {
                    AliceResponse resp = new AliceResponse();
                    resp.Response.Text = "кнопочка";
                    resp.Response.Tts = "кнопочка";

                    resp.Response.buttons = new List<ButtonResponse>();
                    ButtonResponse bnt = new ButtonResponse();
                    bnt.title = "йакнопка";
                    bnt.hide = false;
                    resp.Response.buttons.Add(bnt);
                    return resp;
                }
                else
                {
                    //режим попугая
                    //var response = new AliceResponse(request);
                    var response = new AliceResponse();
                    response.Response.Text = "режим угадайки: " + StorageU.regimUgadaika;
                    return response;
                }
            }
        }
        public string token(string access_token, string expires_in = "", string token_type = "")
        {
            string res = access_token;
            if (res.Contains("#"))
                res = res.Substring(res.IndexOf("#"));
            //hmmm
            throw new Exception(res);

            return res;
        }
    }
}