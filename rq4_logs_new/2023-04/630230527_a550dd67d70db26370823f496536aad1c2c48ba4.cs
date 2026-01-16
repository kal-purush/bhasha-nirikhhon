using ApiStudent.Models;

namespace ApiStudent.Auth
{
    public interface IJwtAuthenticationService
    {
        tokenJwt Authenticate(string username, string password);
    }
}