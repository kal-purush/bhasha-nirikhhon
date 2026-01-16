using System.Net;
using System.Threading.Tasks;

namespace OMineGuard.Backend
{
    public static class Informer
    {
        public static void SendMessage(string Message)
        {
            string message = $"{Settings.Profile.RigName} >> {Message}";
            Task.Run(() => SendVKMessage(message));
            Task.Run(() => SendTelegramMessage(message));
        }

        private static void SendVKMessage(string message)
        {
            if (Settings.Profile.Informer.VkInform)
            {
                string user_id = Settings.Profile.Informer.VKuserID;
                string access_token = "6e8b089ad4fa647f95cdf89f4b14d183dc65954485efbfe97fe2ca6aa2f65b1934c80fccf4424d9788929";
                string ver = "5.73";

                string BaseReq = $"https://api.vk.com/method/messages.send" +
                    $"?user_id={user_id}" +
                    $"&message={message}" +
                    $"&access_token={access_token}" +
                    $"&v={ver}";

                WebRequest.Create(BaseReq).GetResponse();
            }
        }
        private static void SendTelegramMessage(string message)
        {

        }
    }
}