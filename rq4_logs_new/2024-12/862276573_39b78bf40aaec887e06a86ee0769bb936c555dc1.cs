using P7_PSEngine.DTO;
using P7_PSEngine.Data;
using P7_PSEngine.Model;
using P7_PSEngine.Handlers;
using P7_PSEngine.Controllers;
using P7_PSEngine.Services;

namespace P7_PSEngine.API;

public static class TestingEndpointsExt
{
    public static void MapTestingEndpoints(this WebApplication app) 
    {
        app.MapGet("/testing/flushdb", async (IUserRepository user_repo) => {
            await user_repo.RemoveAllUsersAsync();
            return new DataErrorDTO {Data = "DB flushed", Error = ""};
        });

        app.MapGet("/testing/seed/env", async (IUserRepository user_repo, ICloudServiceRepository cloud_repo, IFileInformationRepository file_repo, IInvertedIndexService index) => {
            //This user is used for testing the fetching of files from dropbox
            User user = new User();
            user.UserId = 0;
            user.UserName = app.Configuration["TESTING_ACCOUNT_USERNAME"];
            user.Password = app.Configuration["TESTING_ACCOUNT_PASSWORD"];
            
            
            DataErrorDTO response = FrontendController.HandleSignUp(user, user_repo).Result;

            if (response.Error != "") {
                return response;
            }

            CloudService service = new CloudService();
            service.refresh_token = app.Configuration["TESTING_ACCOUNT_REFRESH_TOKEN_DBOX"];
            service.UserDefinedServiceName = "SeedableDropbox";
            service.User = user;
            service.ServiceType = "dropbox";

            await cloud_repo.AddServiceAsync(service);
            await cloud_repo.SaveDbChangesAsync();

            DropBoxHandler DBHandler = new DropBoxHandler(file_repo, user_repo, cloud_repo, index);

            List<FileInformation> files = await DBHandler.ServiceFileRequest(app, user, null, service);

            if (files.Count == 0) {
                return new DataErrorDTO {Data = "", Error = "Error: User has no files in DBOX or Service file request could not connect"};
            }
            
            if (!DBHandler.ProcessFiles(app, user, null, service, index).Result) {
                return new DataErrorDTO {Data = "", Error = "Error: Indexing failed"};
            }

            return new DataErrorDTO {Data = "seed success", Error = ""};
        });

        app.MapGet("/testing/gettestinguser", () => {
            return new { username = app.Configuration["TESTING_ACCOUNT_USERNAME"], password = app.Configuration["TESTING_ACCOUNT_PASSWORD"] };
        });
    }
}