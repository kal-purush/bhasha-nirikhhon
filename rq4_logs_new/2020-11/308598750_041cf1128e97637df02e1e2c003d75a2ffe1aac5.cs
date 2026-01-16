using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace api.layer.BusinessLayer
{
    public class GitActionsManager : IGitActionsManager
    {
        public async Task<JObject> OpenRequestedCreated(GitActions gitActions)
        {
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("User-Agent", "TODO-App");
                    using (var response = await httpClient.GetAsync(gitActions.pull_request.url))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        var test = JsonConvert.DeserializeObject<JObject>(apiResponse);
                        
                        //SQL Code    

                        return test;
                    }
                }
        }

        public bool PullRequestedCreated(GitActions gitActions)
        {
            return true;
        }
    }
}