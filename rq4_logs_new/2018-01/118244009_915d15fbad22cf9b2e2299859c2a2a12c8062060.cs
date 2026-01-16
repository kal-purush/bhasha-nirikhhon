using Newtonsoft.Json;

namespace Exercise_2.Models
{
    public class UserDetail
    {
        [JsonProperty("login")]
        public string Login { get; set; }

        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("html_url")]
        public string HtmlUrl { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("location")]
        public string Location { get; set; }

        [JsonProperty("email")]
        public string Email { get; set; }

        [JsonProperty("company")]
        public string Company { get; set; }

        [JsonProperty("public_repos")]
        public int PublicRepos { get; set; }

        [JsonProperty("public_gists")]
        public int PublicGists { get; set; }

        public override string ToString() => $"Login: {Login}" +
                                             $"\nId: {Id}" +
                                             $"\nHtmlUrl: {HtmlUrl}" +
                                             $"\nName: {Name}" +
                                             $"\nLocation: {Location}" +
                                             $"\nEmail: {Email}" +
                                             $"\nCompany: {Company}" +
                                             $"\nPublic Repos: {PublicRepos}" +
                                             $"\nPublic Gists: {PublicGists}";
    }
}