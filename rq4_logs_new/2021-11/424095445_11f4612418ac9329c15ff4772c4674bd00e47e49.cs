using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Neiria.Domain.Models;

namespace Neiria.BlazorApp.Data.Services
{
  public class CatergoryService : ICatergoryService
  {
    private readonly HttpClient _httpclient;

    public CatergoryService(HttpClient httpClient)
    {
      _httpclient = httpClient;
    }

    public async Task<IEnumerable<Catergory>> GetAllCatergories()
    {
      return await _httpclient.GetFromJsonAsync<Catergory[]>("api/Catergory");
    }
  }
}
