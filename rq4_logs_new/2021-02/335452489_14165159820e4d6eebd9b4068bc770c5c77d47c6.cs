using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using RestSharp;

namespace DemoDockerTestProject
{
    [TestClass]
    public class DockerDemo
    {
        RestClient client = new RestClient("https://pokeapi.co/api/v2/");

        [TestMethod]
        [Description("Verify get all pokemon")]
        public void GetAllPokemon()
        {
            #region Make Get request
            //Create request with no token
            var request = new RestRequest("pokemon", Method.GET) { RequestFormat = DataFormat.Json };
            request.AddHeader("Content-Type", "application/json");
            //Execute request and verify Unauthorized status
            var response = client.Execute(request);
            Assert.IsTrue(response != null, "No response from poke API call");
            Assert.IsTrue(response.StatusCode.ToString() == "OK", "Did not get expected status returned Expected: OK Actual: {0}", response.StatusCode.ToString());
            var responseObj = JObject.Parse(response.Content);
            Assert.IsNotNull(responseObj["results"]);
            var results = responseObj["results"];
            #endregion

            #region Verify Starters
            Assert.AreEqual(results[0]["name"].ToString(), "bulbasaur");
            Assert.AreEqual(results[3]["name"].ToString(), "charmander");
            Assert.AreEqual(results[6]["name"].ToString(), "squirtle");
            #endregion
        }

        [TestMethod]
        [Description("Verify get specific pokemon")]
        public void GetSinglePokemon()
        {
            #region Test Setup
            string singlePokemon = "pikachu";
            #endregion

            #region Make Get request
            //Create request with no token
            var request = new RestRequest("pokemon/"+ singlePokemon, Method.GET) { RequestFormat = DataFormat.Json };
            request.AddHeader("Content-Type", "application/json");
            //Execute request and verify Unauthorized status
            var response = client.Execute(request);
            Assert.IsTrue(response != null, "No response from poke API call");
            Assert.IsTrue(response.StatusCode.ToString() == "OK", "Did not get expected status returned Expected: OK Actual: {0}", response.StatusCode.ToString());
            var responseObj = JObject.Parse(response.Content);
            #endregion

            #region Verify Abilities
            Assert.IsNotNull(responseObj["abilities"]);
            var abilities = responseObj["abilities"];
            Assert.AreEqual(abilities[0]["ability"]["name"].ToString(), "static");
            Assert.AreEqual(abilities[1]["ability"]["name"].ToString(), "lightning-rod");
            #endregion

        }
    }
}