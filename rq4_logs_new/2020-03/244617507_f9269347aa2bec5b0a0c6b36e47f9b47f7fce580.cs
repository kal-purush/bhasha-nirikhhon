using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using Xunit;

namespace LunaBack.Test
{
   public class AdminTest
   {
       [Fact]
       public async void Delete_Fail()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/delete/35c4a054-271b-4b23-bd4c-44ee36e99e0f");

                Assert.True(res.StatusCode == HttpStatusCode.BadRequest);

                var check = res.Content.ReadAsStringAsync();
                Assert.StartsWith("Invalid", check.Result);

           }
       }

       

       [Fact]
       public async void Activate()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/activate/82bc5ab0-0c6d-41d8-aa76-e00c90fd310a");

               Assert.True(res.IsSuccessStatusCode);

               var check = res.Content.ReadAsStringAsync();
               Assert.NotEmpty(check.Result);

           }
       }


       [Fact]
       public async void Deactivate()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/deactivate/82bc5ab0-0c6d-41d8-aa76-e00c90fd310a");

               Assert.True(res.IsSuccessStatusCode);

               var check = res.Content.ReadAsStringAsync();
               Assert.NotEmpty(check.Result);

           }
       }


       [Fact]
       public async void Deactivate_Fail()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/deactivate/82bc5ab0-0c6d-41d8-aa76-e00c90fd318b");

               Assert.True(res.StatusCode == HttpStatusCode.BadRequest);

               var check = res.Content.ReadAsStringAsync();
               Assert.StartsWith("Invalid", check.Result);

           }
       }

       [Fact]
       public async void Activate_Fail()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/activate/82bc5ab0-0c6d-41d8-aa76-e00c90fd318b");

               Assert.True(res.StatusCode == HttpStatusCode.BadRequest);

               var check = res.Content.ReadAsStringAsync();
               Assert.StartsWith("Invalid", check.Result);

           }
       }

       [Fact]
       public async void Delete()
       {
           using (var context = new HttpClient())
           {
               var res = await context.GetAsync(
                   "http://localhost:5000/api/admin/delete/82bc5ab0-0c6d-41d8-aa76-e00c90fd310a");

               Assert.True(res.IsSuccessStatusCode);

               var check = res.Content.ReadAsStringAsync();
               Assert.NotEmpty(check.Result);

           }
       }

    }
}