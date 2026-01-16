using System.Security.Cryptography;
using System.Text;

namespace FA2.Src.RefreshToken;

public sealed class AppRefreshTokenHandler : IAppRefreshTokenHandler
{
    private const string Chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz!@#$%^&*+=";

    public string Generate(int tokenLength = 15)
    {
        var builder = new StringBuilder();

        for (int time = 0; time < tokenLength; time++)
        {
            builder.Append(Chars[RandomNumberGenerator.GetInt32(Chars.Length)]);
        }

        return builder.ToString();
    }
}