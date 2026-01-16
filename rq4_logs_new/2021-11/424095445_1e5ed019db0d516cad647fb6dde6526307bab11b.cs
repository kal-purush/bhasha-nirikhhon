using Neiria.Domain.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Net.Http.Json;

namespace Neiria.BlazorApp.Data.Services
{
  public class ClothService : IClothService
  {
    private readonly HttpClient _httpclient;

    public ClothService(HttpClient httpClient)
    {
      _httpclient = httpClient;
    }

    public async Task<IEnumerable<Cloth>> GetAllClothes()
    {
      return await _httpclient.GetFromJsonAsync<Cloth[]>("api/Clothes");
    }
  }
}