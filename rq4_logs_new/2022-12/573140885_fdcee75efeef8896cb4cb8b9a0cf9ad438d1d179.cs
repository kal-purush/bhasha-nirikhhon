#nullable disable

namespace Model;

public class Credentials
{
    public CertificateSignature Certificate { get; init; }
    public LoginCredentials Login { get; init; }
    public ClientCredentials Credential { get; init; }

    public class CertificateSignature
    {
        public string Validity { get; init; }
        public string Password { get; init; }
        public string Data { get; init; }
    }

    public class LoginCredentials
    {
        public string User { get; init; }
        public string Password { get; init; }
        public string EncryptedPassword { get; set; }
    }

    public class ClientCredentials
    {
        public string ClientId { get; init; }
        public string ClientSecret { get; init; }
    }
}