using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Kintai.Models.Session;

namespace Kintai.Controllers
{
    public class LoginController : Controller
    {
        public string CallbackUrl => $"{HttpContext.Request.Scheme}://{HttpContext.Request.Host}{Url.Action("Callback", "Login")}";

        public IActionResult Index()
        {
            // セッションからアカウント情報を取得
            var account = HttpContext.Session.GetObject<Account>(Account.SESSION_KEY) as Account;

            // ログイン済みならデフォルト画面へ遷移
            if (account?.IsLogin ?? false) return RedirectToAction("Index", "Home");

            // クッキーからリフレッシュトークンが取得できるならそのままログイン
            if (HttpContext.Request.Cookies["refresh_token"] != null)
            {
                // リフレッシュトークンを使ってアクセストークンを取得
                var token = GoogleOAuth.GetAccessToken(HttpContext.Request.Cookies["refresh_token"]);
                // アクセストークンを使ってログイン処理を実施
                return ExecuteLogin(token);
            }

            // ログインを促す画面を表示
            ViewData["Title"] = "ログイン";
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Post()
        {
            ;
#if DEBUG
            return ExecuteLogin();
#else
            // GoogleのOAuth2.0認証画面にリダイレクト
            return Redirect(GoogleOAuth.GetAuthUrl(CallbackUrl));
#endif
        }

        [HttpGet]
        public IActionResult Callback(string code)
        {
            // GoogleのOAuth認証画面からのリダイレクト処理
            // アクセストークンとリフレッシュトークンを取得
            var token = GoogleOAuth.GetAccessToken(code, CallbackUrl, out string refreshToken);
            // リフレッシュトークンはクッキーに保持
            HttpContext.Response.Cookies.Append("refresh_token", refreshToken);
            // アクセストークンを使ってログイン処理を実施
            return ExecuteLogin(token);
        }

        private ActionResult ExecuteLogin(string accessToken = null)
        {
            var email = "developer@microgear.co.jp";
            var name = "開発 太郎";
            if (accessToken != null)
            {
                email = GoogleOAuth.GetEmail(accessToken);
                name = "後藤 上総"; // ToDo:DBより取得（取得できなければログイン失敗にする）
            }
            var logoutAccount = HttpContext.Session.GetObject<Account>(Account.SESSION_KEY) as LogoutAccount;
            HttpContext.Session.SetObject<Account>(Account.SESSION_KEY, new LoginAccount(email, name));
            if (logoutAccount != null)
            {
                // リダイレクト先があるならその画面へ遷移
                return RedirectToRoute(logoutAccount.RouteValues);
            }
            else
            {
                // 戻り先がないならデフォルト画面へ
                return RedirectToAction("Index", "Home");
            }
        }
    }

    public static class GoogleOAuth
    {
        public static readonly string ClientId = "";
        public static readonly string ClientSecret = "";
        public static string Callback { get; private set; }

        static GoogleOAuth()
        {
            ClientId = "806346248877-lkple2av52ef1m4v43tg6flg97sqrh0e.apps.googleusercontent.com";
            ClientSecret = "H5sJ9N6RznP33g-V0JAJ4n5H";
        }

        public static void Initialize(string callbackUrl)
        {
            Callback = callbackUrl;
        }

        /// <summary>
        /// 認証画面のURLを取得
        /// </summary>
        /// <param name="callback">コールバック</param>
        /// <returns></returns>
        public static string GetAuthUrl(string callback)
        {
            string scope = Uri.EscapeDataString("https://www.googleapis.com/auth/userinfo.email");
            return $"https://accounts.google.com/o/oauth2/auth?scope={scope}&redirect_uri={callback}&response_type=code&client_id={ClientId}&approval_prompt=force&access_type=offline";
        }

        public static string GetAccessToken(string authorizationCode, string callback, out string refreshToken)
        {
            // 送信データ
            byte[] postDataBytes = Encoding.UTF8.GetBytes(
                $"code={authorizationCode}&client_id={ClientId}&client_secret={ClientSecret}&redirect_uri={callback}&grant_type=authorization_code");

            // トークンを取得
            var tokenObj = GetToken(postDataBytes);

            // リフレッシュトークンはoutパラメータに、アクセストークンは戻り値に設定
            refreshToken = tokenObj["refresh_token"].ToString();
            return tokenObj["access_token"].ToString();
        }

        public static string GetAccessToken(string refreshToken)
        {
            // 送信データ
            byte[] postDataBytes = Encoding.UTF8.GetBytes(
                $"refresh_token={refreshToken}&client_id={ClientId}&client_secret={ClientSecret}&grant_type=refresh_token");

            // トークンを取得
            var tokenObj = GetToken(postDataBytes);

            // アクセストークンを戻り値に設定
            return tokenObj["access_token"].ToString();
        }

        public static string GetEmail(string accessToken)
        {
            // アクセストークンを使いメールアドレスを取得
            var uri = new Uri("https://www.googleapis.com/oauth2/v2/userinfo?access_token=" + accessToken);
            var req = (HttpWebRequest)WebRequest.Create(uri);

            // レスポンスの取得 
            var res = req.GetResponse();
            var resStream = res.GetResponseStream();
            var sr = new System.IO.StreamReader(resStream, System.Text.Encoding.GetEncoding("UTF-8"));
            var jsonStr = sr.ReadToEnd();
            sr.Close();
            var jObject = JsonConvert.DeserializeObject(jsonStr) as JObject;
            return jObject["email"].ToString();
        }


        private static JObject GetToken(byte[] postData)
        {
            // 送信先の設定 
            Uri uri = new Uri("https://accounts.google.com/o/oauth2/token");
            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(uri);
            req.Method = "POST";
            req.ContentType = "application/x-www-form-urlencoded";
            req.ContentLength = postData.Length;
            req.AllowAutoRedirect = false;

            // リクエストの送信 
            System.IO.Stream reqStream = req.GetRequestStream();
            reqStream.Write(postData, 0, postData.Length);
            reqStream.Close();

            // レスポンスの取得 
            System.Net.WebResponse res = req.GetResponse();
            System.IO.Stream resStream = res.GetResponseStream();
            System.IO.StreamReader sr = new System.IO.StreamReader(resStream, System.Text.Encoding.GetEncoding("UTF-8"));
            string jsonStr = sr.ReadToEnd();
            sr.Close();

            // レスポンスからトークンを取得
            return JsonConvert.DeserializeObject(jsonStr) as JObject;
        }
    }
}