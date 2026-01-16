using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Windows.Storage;
using static Google.Apis.Drive.v3.FilesResource;
using FileManager.Helpers;
using FileManager.Models;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Newtonsoft.Json;
using ThirdPartyServices.Shared.Models.Responses.Microsoft;
using ThirdPartyServices.UWP.CloudServices;
using ThirdPartyServices.Shared.Interfaces;
using FileManager.Dependencies;
using Autofac;
using Newtonsoft.Json.Linq;
using ThirdPartyServices.UWP.AuthorizationServices;
using ThirdPartyServices.Shared.Models;
using ThirdPartyServices.Shared.Models.Parameters;
using ThirdPartyServices.Shared.Models.Requests;
using System.Net.Http.Json;

namespace FileManager.Services
{
    public class OneDriveService
    {
        private readonly IOneDriveCloudService oneDriveCloudService;
        private readonly IMicrosoftAuthorizationService microsoftAuthService;
        public OneDriveService()
        {
            microsoftAuthService = VMDependencies.Container.Resolve<IMicrosoftAuthorizationService>();
            oneDriveCloudService = VMDependencies.Container.Resolve<IOneDriveCloudService>();
        }
        public async Task<string> CheckInternetConnectionAsync(string source)
        {
            string result;
            using (var client = new HttpClient())
            {
                try
                {
                    var responce = await client.GetAsync(source).ConfigureAwait(true);
                    result = Constants.Success;
                }
                catch (HttpRequestException)
                {
                    result = Constants.Failed;
                }
            }
            return result;
        }
        public async Task<string> OneDriveAuthAsync(MicrosoftAuthParams microsoftParams, TokenResult accessToken)
        {
            string result;
            try
            {
                var token = await microsoftAuthService.AuthorizeAsync(microsoftParams);
                accessToken.Access_token = token.Token;
                accessToken.Refresh_token = token.RefreshToken;
                accessToken.Expires_in = token.ExpiresIn.ToString();
                accessToken.LastRefreshTime = DateTime.Now;
                accessToken.Token_type = "Bearer";
                result = Constants.Success;
            }
            catch (Exception)
            {
                result = Constants.Failed;
            }
            return result;
        }
        public async Task<ItemsResponse> GetFilesAsync(string folderId, string accessToken)
        {
            ItemsResponse items;
            try
            {
                items = await oneDriveCloudService.GetFilesByFolderIdAsync(accessToken, folderId);
            }
            catch 
            {
                items = null;
            }
            return items;
        }
        public async Task<string> DownloadFileAsync(StorageFolder destinationFolder, string fileName, string fileId, string accessToken)
        {
            string result = Constants.Failed;
            if (destinationFolder != null)
            {
                StorageFile destinationFile = await destinationFolder.CreateFileAsync(
                        fileName, CreationCollisionOption.GenerateUniqueName);

                var file = await oneDriveCloudService.DownloadItemAsync(new DownloadParams
                {
                    Token = accessToken,
                    FileName = fileId,
                });
                if (file.Name != null)
                {
                    try
                    {
                        await FileIO.WriteBytesAsync(destinationFile, (file.StreamContent as MemoryStream).ToArray());
                        result = Constants.Success;
                    }
                    catch (Exception)
                    {
                        await destinationFile.DeleteAsync();
                        result = Constants.Failed;
                    }
                }
                else
                {
                    await destinationFile.DeleteAsync();
                    result = Constants.Failed;
                }                
            }
            return result;
        }
        public async Task<string> UploadFileAsync(StorageFile uploadFile, string parentId, string accessToken)
        {
            string result = Constants.Failed;            

            if (!string.IsNullOrEmpty(accessToken) && uploadFile != null)
            {
                var streamData = await uploadFile.OpenReadAsync();
                var stream = streamData.AsStreamForRead();
                try
                {
                    await oneDriveCloudService.UploadItemAsync(stream, new UploadParams 
                    { 
                        Token = accessToken, 
                        FileName = uploadFile.Name,    
                    });
                    result = Constants.Success;
                }
                catch (Exception)
                {
                    result = Constants.Failed;
                }                
            }

            //var credentional = GoogleCredential.FromAccessToken(accessToken).CreateScoped(DriveService.Scope.Drive);
            //using (var service = new DriveService(new BaseClientService.Initializer()
            //{
            //    HttpClientInitializer = credentional
            //}))
            //{
            //    if (uploadFile != null)
            //    {
            //        var fileMetadata = new Google.Apis.Drive.v3.Data.File()
            //        {
            //            Name = uploadFile.Name,
            //            Parents = parents
            //        };
            //        var stream = await uploadFile.OpenStreamForReadAsync().ConfigureAwait(true);
            //        var request = service.Files.Create(fileMetadata, stream, Constants.OctetContentType);
            //        request.Fields = "*";
            //        var results = await request.UploadAsync().ConfigureAwait(true);

            //        if (results.Status != Google.Apis.Upload.UploadStatus.Failed)
            //        {
            //            result = Constants.Success;
            //        }
            //    }
            //}
            return result;
        }
        public async Task<string> DeleteFileAsync(string fileId, string accessToken)
        {
            string result = Constants.Failed;
            if (fileId != null)
            {
                try
                {
                    await oneDriveCloudService.DeleteFileAsync(accessToken, fileId);
                    result = Constants.Success;
                }
                catch (HttpRequestException)
                {
                    result = Constants.Failed;
                }
            }
            return result;
        }
        public async Task<string> CreateNewFolderAsync(string folderName, string parent, string tokenType, string accessToken)
        {
            string result;
            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(tokenType, accessToken);
                string source = $"https://graph.microsoft.com/v1.0/me/drive/items/{parent}/children";
                var folderObject = JObject.FromObject(new
                {
                    name = folderName,
                    folder = new { },
                });
                HttpContent content = new StringContent(folderObject.ToString(), Encoding.UTF8, "application/json");
                try
                {
                    HttpResponseMessage response = await client.PostAsync(source, content);
                    result = Constants.Success;
                }
                catch (HttpRequestException)
                {
                    result = Constants.Failed;
                }
                finally
                {
                    content.Dispose();
                }
            }
            return result;
        }
        public async Task<string> RenameFileAsync(string itemId, string fileName, string tokenType, string accessToken)
        {
            string result;
            string source = $"https://graph.microsoft.com/v1.0/me/drive/items/{itemId}";
            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(tokenType, accessToken);
                var folderObject = JObject.FromObject(new
                {
                    name = fileName,
                });
                HttpContent content = new StringContent(folderObject.ToString(), Encoding.UTF8, "application/json");
                var method = "PATCH";
                var httpVerb = new HttpMethod(method);
                HttpRequestMessage httpRequestMessage = new HttpRequestMessage(httpVerb, source)
                {
                    Content = content,
                };                
                try
                {
                    HttpResponseMessage response = await client.SendAsync(httpRequestMessage);
                    result = Constants.Success;
                }
                catch (HttpRequestException)
                {
                    result = Constants.Failed;
                }
                finally
                {
                    content.Dispose();
                }
            }
            return result;
        }
        public async Task<string> RefreshTokenAsync(MicrosoftAuthParams microsoftParams, TokenResult accessToken)
        {
            string result = string.Empty;
            if (accessToken != null)
            {
                try
                {
                    var token = await microsoftAuthService.ExchangeRefreshTokenAsync(microsoftParams, accessToken.Refresh_token);
                    accessToken.Access_token = token.Result.AccessToken;
                    accessToken.Expires_in = token.Result.ExpiresIn.ToString();
                    accessToken.Refresh_token = token.Result.RefreshToken;
                    accessToken.LastRefreshTime = DateTime.Now;
                    result = Constants.Success;
                }
                catch (Exception)
                {
                    result = Constants.Failed;
                }
            }
            return result;
        }
    }
}