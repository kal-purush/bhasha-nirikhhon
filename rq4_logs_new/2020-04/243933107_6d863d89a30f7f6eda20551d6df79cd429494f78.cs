using Refit;
using System;
using System.Threading.Tasks;
using TESTAPI.Contract.V1.Requests;

namespace TESTAPI.SDK.SAMPLE
{
    class Program
    {
        static async  Task Main(string[] args)
       {
            var cachedToken = string.Empty;

            var identityApi = RestService.For<IIdentityApi>("http://localhost:51858");
            var postApi = RestService.For<IPostApi>("http://localhost:51858", new RefitSettings 
            { 
            AuthorizationHeaderValueGetter = () => Task.FromResult(cachedToken)
            });


            var registerResponse = await identityApi.RegisterAsync(new UserRegistrationReqest
            {
                Email = "sumidu@gmail.com",
                Password = "Test1234!"

            });

            var userloginResponse = await identityApi.LoginAsync(new UserLoginReqest
            {
                Email = "sumidu@gmail.com",
                Password = "Test1234!"

            });

            cachedToken = userloginResponse.Content.Token;

            var allpost = await postApi.GetAllAsync();

            var createdPost = await postApi.CreateAsync(new CreatePostReqest
            {
                Name = "This is created by sdk"
            });

            var receviedPost = await postApi.GetAsync(createdPost.Content.Id);


            var updatePost = await postApi.UpdateAsync(createdPost.Content.Id, new UpdatePostReqest
            {

                Name = "This is updated by the SDK"
            });

            var deletePost = await postApi.DeleteAsync(createdPost.Content.Id);

        }
    }
}