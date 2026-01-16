using PayPal.Api;

namespace Appota.Models
{
    public class PaypalConfiguration
    {
        //Variables for storing the clientID and clientSecret key  
        public  static string ClientId;
        public  static string ClientSecret;

        //Constructor  
        static PaypalConfiguration()
        {
            ClientId = "AT1Z4fukZnTi1LAyxmXPLXuN2ZnZK6CZy7Ai3Dy6OJ41Y4MX7C6QtMpW3cOvDd3CQ19_JeiqEO9fJh-t";
            ClientSecret = "ELv_kvE3bUZ7n3sQnj_lsP179Nk3tkl5aaN_1Y5PaLGxkXFke84pa5N66DcNDS2Y_jwop4gtAQOngs1L";
        }
        // getting properties from the web.config  
        public static Dictionary<string, string> GetConfig()
        {
            return PayPal.Api.ConfigManager.Instance.GetProperties();
        }
        private static string GetAccessToken()
        {
            // getting accesstocken from paypal  
            string accessToken = new OAuthTokenCredential(ClientId, ClientSecret, GetConfig()).GetAccessToken();
            return accessToken;
        }
        public static APIContext GetAPIContext()
        {
            // return apicontext object by invoking it with the accesstoken  
            APIContext apiContext = new APIContext(GetAccessToken());
            apiContext.Config = GetConfig();
            return apiContext;
        }
    }
}