using EtudeManyToMany.Core.Model;
using System.Linq.Expressions;
using System.Net.Http.Json;

namespace EtudeManyToMany.Blazor.Services
{

    public class APIConducteurService : IService<Conducteur>
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseApiRoute; // = "http://localhost:7044/api/conducteurs";

        public APIConducteurService(HttpClient httpClient, IConfiguration configuration)
        {
            _httpClient = httpClient;
            _baseApiRoute = configuration["ConducteurAPIUrlHttps"] + "/api/conducteurs";
        }
        public async Task<bool> Add(Conducteur conducteur)
        {
            var result = await _httpClient.PostAsJsonAsync(_baseApiRoute, conducteur);
            return result.IsSuccessStatusCode;
        }

        public async Task<bool> Delete(int id)
        {
            var result = await _httpClient.DeleteAsync(_baseApiRoute + $"/{id}");
            return result.IsSuccessStatusCode;
        }

        public async Task<Conducteur?> Get(Expression<Func<Conducteur, bool>> predicate)
        {
            var passagers = await GetAll(); // Obtenez tous les passagers
            return passagers.FirstOrDefault(predicate.Compile()); // Appliquez le prédicat sur la liste de passagers
        }

        public async Task<List<Conducteur>> GetAll()
        {
            var result = await _httpClient.GetFromJsonAsync<List<Conducteur>>(_baseApiRoute);
            return result!;
        }

        public async Task<List<Conducteur>> GetAll(Expression<Func<Conducteur, bool>> predicate)
        {
            var conducteurs = await GetAll(); // Obtenez tous les passagers
            return conducteurs.Where(predicate.Compile()).ToList(); // Appliquez le prédicat sur la liste de passagers
        }

        public async Task<Conducteur?> GetById(int id)
        {
            var result = await _httpClient.GetFromJsonAsync<Conducteur>(_baseApiRoute + $"/{id}");
            return result;
        }

        public async Task<bool> Update(Conducteur conducteur)
        {
            var result = await _httpClient.PutAsJsonAsync(_baseApiRoute + $"/{conducteur.ConducteurId}", conducteur);
            return result.IsSuccessStatusCode;
        }
    }
}