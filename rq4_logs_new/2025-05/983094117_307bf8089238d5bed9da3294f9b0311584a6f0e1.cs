using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using ServiPuntos.uy_mobile.Services.Interfaces;
using Microsoft.Maui.Storage;
using ServiPuntos.uy_mobile.Models;

namespace ServiPuntos.uy_mobile.Services;

public partial class ApiService : IApiService
{
  private readonly HttpClient _httpClient;
  private readonly IConfiguration _configs;

  public ApiService(IConfiguration configuration)
  {
    _configs = configuration;
    _httpClient = new()
    {
      BaseAddress = new Uri(_configs["API_URL"]!),
    };
    _httpClient.DefaultRequestHeaders.Add("X-Tenant-Id", _configs["TENANT_ID"]);
  }

  public async Task<ApiResponse<T>> GET<T>(string requestUri)
  {
    try
    {
      var storedToken = await GetTokenAsync();
      if (storedToken != null)
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", storedToken);
      var response = await _httpClient.GetAsync(requestUri);
      var responseContent = await response.Content.ReadAsStringAsync();
      return JsonConvert.DeserializeObject<ApiResponse<T>>(responseContent) ?? new ApiResponse<T>(true, default, "Error interno, intenta nuevamente más tarde.");
    }
    catch (Exception ex)
    {
      return new ApiResponse<T>(true, default, ex.Message);
    }
  }

  public async Task<ApiResponse<T>> POST<T>(string requestUri, dynamic requestData)
  {
    try
    {
      var storedToken = await GetTokenAsync();
      if (storedToken != null)
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", storedToken);
      var content = new StringContent(JsonConvert.SerializeObject(requestData), Encoding.UTF8, "application/json");
      var response = await _httpClient.PostAsync(requestUri, content);
      var responseContent = await response.Content.ReadAsStringAsync();
      return JsonConvert.DeserializeObject<ApiResponse<T>>(responseContent) ?? new ApiResponse<T>(true, default, "Error interno, intenta nuevamente más tarde.");
    }
    catch (Exception ex)
    {
      return new ApiResponse<T>(true, default, ex.Message);
    }
  }

  public async Task<ApiResponse<T>> PATCH<T>(string requestUri, dynamic requestData)
  {
    try
    {
      var storedToken = await GetTokenAsync();
      if (storedToken != null)
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", storedToken);
      var content = new StringContent(JsonConvert.SerializeObject(requestData), Encoding.UTF8, "application/json");

      var request = new HttpRequestMessage(new HttpMethod("PATCH"), requestUri)
      {
        Content = content
      };
      var response = await _httpClient.SendAsync(request);
      var responseContent = await response.Content.ReadAsStringAsync();
      return JsonConvert.DeserializeObject<ApiResponse<T>>(responseContent) ?? new ApiResponse<T>(true, default, "Error interno, intenta nuevamente más tarde.");
    }
    catch (Exception ex)
    {
      return new ApiResponse<T>(true, default, ex.Message);
    }
  }

  private static async Task<string?> GetTokenAsync()
  {
    string? sessionJson = await SecureStorage.GetAsync("SessionData");
    if (string.IsNullOrWhiteSpace(sessionJson))
      return null;
    var sessionData = JsonConvert.DeserializeObject<SessionData>(sessionJson);
    return sessionData?.Token;
  }
}