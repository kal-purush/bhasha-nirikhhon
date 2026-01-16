using JRequest.Net.Enumerators;
using System;
using System.Collections.Generic;
using System.Text;

namespace JRequest.Net
{
    public class Authorization
    {
        public string Type { get; set; } = AuthType.NoAuth.ToString();
        public string Token { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public static void AddAuthorizationHeader(Request request)
        {
            string token = string.Empty;
            try
            {
                switch (request.Authorization.Type.ToLower())
                {
                    case "basic":
                        if (!Utility.HasValue(request.Authorization.Token) && (Utility.HasValue(request.Authorization.Username) && (Utility.HasValue(request.Authorization.Password))))
                        {
                            string base64Token = ToBase64(request.Authorization.Username, request.Authorization.Password);
                            token = $"{TokenExtension.Basic} {base64Token}";
                        }
                        else if (Utility.HasValue(request.Authorization.Token))
                        {
                            token = $"{TokenExtension.Basic} {request.Authorization.Token}";
                        }
                        break;
                    case "bearer":
                        if (Utility.HasValue(request.Authorization.Token))
                        {
                            token = $"{TokenExtension.BearerToken} {request.Authorization.Token}";
                        }
                        break;
                    default:
                        break;
                }

                if (Utility.HasValue(token))
                {
                    Dictionary<string, string> authDic = new Dictionary<string, string>()
                {
                    { "Authorization", token}
                };

                    request.Headers.Add(authDic);
                }
            }
            catch (Exception ex)
            {

                throw new JRequestException(ex.Message, ex.InnerException);
            }

        }

        public static string ToBase64(string username, string password)
        {
            var combinedTxt = $"{username}:{password}";
            var plainTextBytes = Encoding.UTF8.GetBytes(combinedTxt);
            return Convert.ToBase64String(plainTextBytes);
        }
    }
}