using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace CoreService.Test
{
    public abstract class BaseGlobalTest(CreateWebApplicationFactory webApplicationFactory) : IClassFixture<CreateWebApplicationFactory>
    {
        protected readonly HttpClient _httpClient = webApplicationFactory.CreateClient();

        protected static readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions()
        {
            MaxDepth = 4,
            PropertyNameCaseInsensitive = true
        };
    }
}