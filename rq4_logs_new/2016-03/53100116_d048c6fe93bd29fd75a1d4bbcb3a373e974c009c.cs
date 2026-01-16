using System;
using System.Configuration;
using System.IdentityModel.Tokens;
using Microsoft.Owin.Security;
using Microsoft.Owin.Security.DataHandler.Encoder;
using Thinktecture.IdentityModel.Tokens;

namespace HQF.Tutorial.WebAPI.Indentity2.Providers
{
    public class CustomJwtFormat : ISecureDataFormat<AuthenticationTicket>
    {
        private readonly string _issuer = string.Empty;

        public CustomJwtFormat(string issuer)
        {
            _issuer = issuer;
        }

        public string Protect(AuthenticationTicket data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }
            //this API serves as Resource and Authorization Server at the same time, so we are fixing the Audience Id and Audience Secret (Resource Server) in web.config file, this Audience Id and Secret will be used for HMAC265 and hash the JWT token, I’ve used this implementation to generate the Audience Id and Secret.
            //https://github.com/tjoudeh/JWTAspNetWebApi/blob/master/AuthorizationServer.Api/AudiencesStore.cs#l24-34
            var audienceId = ConfigurationManager.AppSettings["as:AudienceId"];

            var symmetricKeyAsBase64 = ConfigurationManager.AppSettings["as:AudienceSecret"];

            //Then we prepare the raw data for the JSON Web Token which will be issued to the requester by providing the issuer, audience, user claims, issue date, expiry date, and the signing key which will sign (hash) the JWT payload.
            var keyByteArray = TextEncodings.Base64Url.Decode(symmetricKeyAsBase64);

            var signingKey = new HmacSigningCredentials(keyByteArray);

            var issued = data.Properties.IssuedUtc;

            var expires = data.Properties.ExpiresUtc;

            var token = new JwtSecurityToken(_issuer, audienceId, data.Identity.Claims, issued.Value.UtcDateTime,
                expires.Value.UtcDateTime, signingKey);

            var handler = new JwtSecurityTokenHandler();

            var jwt = handler.WriteToken(token);

            return jwt;
        }

        public AuthenticationTicket Unprotect(string protectedText)
        {
            throw new NotImplementedException();
        }
    }
}