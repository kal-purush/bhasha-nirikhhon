using System.Net.Http;
using System.Text;
using AgentApplication.ClassLib.Database.Infrastructure;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace AgentApplication.API.Controllers.Base
{
    public class BaseApiController : ControllerBase
    {
        protected string _dislinktApiGatewayBaseUrl => "http://localhost:8000/";
        protected static HttpClient _httpClient = new ();
        protected readonly IUnitOfWork _uow;
        protected readonly IMapper _mapper;
        protected readonly IConfiguration _config;

        public BaseApiController(IUnitOfWork uow, IMapper mapper, IConfiguration config)
        {
            _uow = uow;
            _mapper = mapper;
            _config = config;
        }

        protected StringContent GetContent(object content)
        {
            return new StringContent(JsonConvert.SerializeObject(content, settings: new JsonSerializerSettings()
            {
                ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore,
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            }), Encoding.UTF8, "application/json");
        }
    }
}