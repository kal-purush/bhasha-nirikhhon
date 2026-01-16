using System;
using MaterialSkin;
using MaterialSkin.Controls;
using Google.GData.Client;
using System.Diagnostics;
using Google.GData.Spreadsheets;
using System.Windows.Forms;

namespace DATAO
{
    public partial class StartForm : MaterialForm
    {
        OAuth2Parameters parameters = new OAuth2Parameters();
        public StartForm()
        {
            InitializeComponent();
            //Служебные поля для авторизации
            string CLIENT_ID = "144023824946-13eih1nnnlh8oq9ae8lmdaamchj8bc69.apps.googleusercontent.com";
            string CLIENT_SECRET = "96MURyKXl8uApYvdn6fWO_3T";
            string SCOPE = "https://spreadsheets.google.com/feeds https://docs.google.com/feeds";
            string REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

            //Заполняем нужные для авторизации параметры (самый минимум)
            parameters.ClientId = CLIENT_ID;
            parameters.ClientSecret = CLIENT_SECRET;
            parameters.RedirectUri = REDIRECT_URI;
            parameters.Scope = SCOPE;
            string authorizationUrl = OAuthUtil.CreateOAuth2AuthorizationUrl(parameters);

            //Открываем у пользователя браузер
            //Process.Start(authorizationUrl);
        }

        private void StartForm_Load(object sender, EventArgs e)
        {

        }

        private void materialFlatButton1_Click(object sender, EventArgs e)
        {
            /*
            parameters.AccessCode = gAuthFormTextField.Text;
            OAuthUtil.GetAccessToken(parameters);

            GOAuth2RequestFactory requestFactory =
                new GOAuth2RequestFactory(null, "DATAO", parameters);
                */
            SpreadsheetsService service = new SpreadsheetsService("DATAO");
            //service.RequestFactory = requestFactory;
            service.setUserCredentials("dataodevelop", "calendarapp");

            SpreadsheetQuery query = new SpreadsheetQuery();
            SpreadsheetFeed feed = service.Query(query);

            //Ищем таблицу с именем datao.init
            //Ее в последствии положим в spreadsheet
            SpreadsheetEntry spreadsheet = new SpreadsheetEntry();
            foreach (SpreadsheetEntry entry in feed.Entries)
            {
                if (entry.Title.Text == "datao.init")
                {
                    spreadsheet = entry;
                }
            }

            //Разбиваем полученную таблицу на листы
            WorksheetFeed wsFeed = spreadsheet.Worksheets;

            //Берем первый лист, там содержится ID, Имя, Фамилия и Дата регистрации
            WorksheetEntry worksheet = (WorksheetEntry)wsFeed.Entries[0];

            //Стандартная операция запроса сетки таблицы от Google
            AtomLink listFeedLink = worksheet.Links.FindService(GDataSpreadsheetsNameTable.ListRel, null);
            ListQuery listQuery = new ListQuery(listFeedLink.HRef.ToString());
            ListFeed listFeed = service.Query(listQuery);

            //Берем первый ряд
            ListEntry row = (ListEntry)listFeed.Entries[0];

            //Проверяем первую букву ID (можно придумать что угодно)
            switch (row.Elements[0].Value.ToString()[0])
            {
                case ('a'):
                    {
                        //a - admin
                        //Открывается форма админа.
                        MessageBox.Show($"Приветствуем, {row.Elements[1].Value.ToString()}");
                        break;
                    }
                case ('w'):
                    {
                        //w - worker
                        //Открывается форма рабочего
                        MessageBox.Show($"Приветствуем, {row.Elements[1].ToString()}");
                        break;
                    }
                default:
                    {
                        //Неизвестный тип
                        break;
                    }
            }

            //Далее в зависимости от типа можно загрузить его страницу на диске, предварительно как-нибудь ее обозвав
            Close();
        }

        private void pictureBox1_Click(object sender, EventArgs e)
        {

        }
    }
}