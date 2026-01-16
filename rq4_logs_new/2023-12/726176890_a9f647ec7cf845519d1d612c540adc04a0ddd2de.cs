using MagicVilla_Web.Models;
using MagicVilla_Web.Models.DTO;
using MagicVilla_Web.Services.IServices;
using static MagicVilla_Utility.SD;

namespace MagicVilla_Web.Services
{
    public class AuthService : BaseService, IAuthService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly string _villaUrl;

        public AuthService(IHttpClientFactory httpClientFactory, IConfiguration configuration)
            : base(httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
            _villaUrl = configuration.GetValue<string>("ServiceUrls:VillaAPI")!;
        }

        public Task<T> LoginAsync<T>(LoginRequestDTO login)
        {
            return SendAsync<T>(new APIRequest
            {
                ApiType = ApiType.POST,
                Data = login,
                Url = _villaUrl + "/api/UsersAuth/login",
            });
        }

        public Task<T> RegisterAsync<T>(RegistrationRequestDTO registration)
        {
            return SendAsync<T>(new APIRequest
            {
                ApiType = ApiType.POST,
                Data = registration,
                Url = _villaUrl + "/api/UsersAuth/register",
            });
        }
    }
}