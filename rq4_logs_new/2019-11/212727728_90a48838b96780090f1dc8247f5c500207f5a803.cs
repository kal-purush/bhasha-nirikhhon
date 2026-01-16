namespace TestWebAPI.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;

    using Microsoft.AspNetCore.Mvc;

    using Newtonsoft.Json;

    using TestWebAPI.DTOs;

    /// <summary>
    /// The values controller.
    /// </summary>
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        /// <summary>
        /// The get.
        /// </summary>
        /// <param name="resultCount">
        /// The result Count.
        /// </param>
        /// <returns>
        /// The <see cref="ActionResult"/>.
        /// </returns>
        [HttpGet]
        public async Task<ActionResult> Get([FromQuery] int resultCount = default)
        {
            if (resultCount == default)
            {
                resultCount = 10;
            }

            var users = new List<User>();

            // https://randomuser.me/api/?results=10
            using (var client = new HttpClient()
                                    {
                                        BaseAddress = new Uri("https://randomuser.me"),
                                        Timeout = TimeSpan.FromMinutes(10)
                                    })
            {
                // client.DefaultRequestHeaders.Add("Authorization", $"Bearer {Token}");
                // client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Token);
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                
                var result = await client.GetAsync($"api/?results={resultCount}");

                if (result != null && result.IsSuccessStatusCode)
                {
                    var response = await result.Content.ReadAsStringAsync();
                    var userApiResponse = JsonConvert.DeserializeObject<UserApiResponse>(response);
                    users.AddRange(userApiResponse.Results);
                }
            }

            return this.Ok(users);
        }
        
        /// <summary>
        /// The post.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        /// <summary>
        /// The put.
        /// </summary>
        /// <param name="id">
        /// The id.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        /// <summary>
        /// The delete.
        /// </summary>
        /// <param name="id">
        /// The id.
        /// </param>
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}