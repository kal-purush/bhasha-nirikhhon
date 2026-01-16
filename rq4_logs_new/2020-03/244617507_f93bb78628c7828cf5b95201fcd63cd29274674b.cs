using System;
using System.Net;
using System.Net.Http;
using Lunabank.Data.Entities;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace LunaBack.Test
{
    public class AccountTest
    {
        [Fact]
        public async void GetAllAccountPass_Test()
        {
            using (var context = new HttpClient())
            {
                var response = await context.GetAsync(" http://localhost:5000/api/account?pageSize=3&PageIndex=1");
                Assert.True(response.IsSuccessStatusCode);
                var content = response.Content.ReadAsStringAsync();
                Assert.NotNull(content);
                
            }
           
        }

        [Fact]
        public async void GetAccountPass_Test()
        {
            using (var context = new HttpClient())
            {
                var response = await context.GetAsync(" http://localhost:5000/api/account/d58a53e9-5b26-4dff-be43-390825839a3d");
                Assert.True(response.IsSuccessStatusCode);
                var content = response.Content.ReadAsStringAsync();
                Assert.NotNull(content);

            }

        }
        [Fact]
        public async void GetAccountFailed_Test()
        {
            using (var context = new HttpClient())
            {
                var response = await context.GetAsync(" http://localhost:5000/api/account/d58a53e9-5b26-4dff-be43-390825839a36");
                Assert.True(response.StatusCode == HttpStatusCode.NotFound);
                var content = response.Content.ReadAsStringAsync();
                Assert.StartsWith("Account",content.Result);

            }

        }

    }
}