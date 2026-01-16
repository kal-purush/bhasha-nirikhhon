using P7_PSEngine.API;
using P7_PSEngine.DTO;
using P7_PSEngine.Repositories;
using Newtonsoft;
using System.Text.Json;
using System;
using System.Collections.Generic;
using P7_PSEngine.Model;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using P7_PSEngine.Migrations;

namespace P7_PSEngine.Handlers;

public class DropBoxHandler : ICloudServiceHandler 
{
    private readonly IFileInformationRepository _file_repo;
    private readonly IUserRepository _user_repo;

    private readonly ICloudServiceRepository _cloud_repo;

    public DropBoxHandler(IFileInformationRepository file_repo, IUserRepository user_repo, ICloudServiceRepository cloud_repo) 
    {
        _user_repo = user_repo;
        _cloud_repo = cloud_repo;
        _file_repo = file_repo;
    }

    private async Task<bool> FetchUserAvatar(WebApplication app, string access_token) {
        var user_request = HttpHandler.JSONAsyncPost(new {query = "foo"}, "https://api.dropboxapi.com/2/check/user", access_token);
        return (user_request.Result.Data == "{\"result\":\"foo\"}");
    }

    private async Task<string> GetAccessToken (WebApplication app, string refresh_token) {
        var refresh_token_body = new FormUrlEncodedContent([
            new KeyValuePair<string, string>("grant_type", "refresh_token"),
            new KeyValuePair<string, string>("refresh_token", refresh_token),
            new KeyValuePair<string, string>("client_id", app.Configuration["DBOX_ID"]!),
            new KeyValuePair<string, string>("client_secret", app.Configuration["DBOX_SECRET"]!),
        ]);

        var dbox_oauth2_task = HttpHandler.FormUrlEncodedAsyncPost(refresh_token_body, "https://api.dropboxapi.com/oauth2/token");
        var dbox_oauth2_response = dbox_oauth2_task.Result;

        string access_token = "";
        if (dbox_oauth2_response.Error == "") {
            DBoxOAuth2DTO response = JsonSerializer.Deserialize<DBoxOAuth2DTO>(dbox_oauth2_response.Data);
            access_token = response.access_token;
        }

        return access_token;
    }

    public async Task<bool> OAuth2Handler(WebApplication app, ServiceCreationDetailsDTO service) 
    {
        //Initial token
        var initial_token_body = new FormUrlEncodedContent([
            new KeyValuePair<string, string>("code", service.code),
            new KeyValuePair<string, string>("grant_type", "authorization_code"),
            new KeyValuePair<string, string>("redirect_uri", app.Configuration["STD_REDIRECT_URI"]!),
            new KeyValuePair<string, string>("client_id", app.Configuration["DBOX_ID"]!),
            new KeyValuePair<string, string>("client_secret", app.Configuration["DBOX_SECRET"]!),
        ]);
        Task<DataErrorDTO> dbox_oauth2_task = HttpHandler.FormUrlEncodedAsyncPost(initial_token_body, "https://api.dropboxapi.com/oauth2/token");
        DataErrorDTO dbox_oauth2_response = dbox_oauth2_task.Result;

        string refresh_token = "";
        if (dbox_oauth2_response.Error == "") {
            DBoxOAuth2DTO response = JsonSerializer.Deserialize<DBoxOAuth2DTO>(dbox_oauth2_response.Data);
            refresh_token = response.refresh_token;
        }

        //Refresh token confirmation
        string access_token = GetAccessToken(app, refresh_token).Result;

        if (dbox_oauth2_response.Error == "") {
            DBoxOAuth2DTO response = JsonSerializer.Deserialize<DBoxOAuth2DTO>(dbox_oauth2_response.Data);
            access_token = response.access_token;
        }

        User? user = _user_repo.GetUserByUsernameAsync(service.user).Result;

        if (user != null && await FetchUserAvatar(app, access_token)) {
            var cloud_service = new CloudService();
            cloud_service.ServiceType = "dropbox";
            cloud_service.UID = user;
            cloud_service.UserDefinedServiceName = service.user_defined_service_name;
            cloud_service.refresh_token = refresh_token;
            await _cloud_repo.AddServiceAsync(cloud_service);
            await _cloud_repo.SaveDbChangesAsync();
            
            return true;
        } else {

            return false; 
        }

    }

    public async Task<List<FileInformation>> ServiceFileRequest(WebApplication app, User user, string? path, CloudService service) 
    {

        string access_token = GetAccessToken(app, service.refresh_token).Result;

        var file_request_object = new {

            include_deleted = false,
            include_has_explicit_shared_members = false,
            include_media_info = false,
            include_mounted_folders = true,
            include_non_downloadable_files = true,
            path = path ?? "",
            recursive = true
        };

        var file_request_task = HttpHandler.JSONAsyncPost(file_request_object, "https://api.dropboxapi.com/2/files/list_folder", access_token);
        
        dynamic files = Newtonsoft.Json.JsonConvert.DeserializeObject(file_request_task.Result.Data);

        List<FileInformation> filelist = new List<FileInformation>();
        //Cursed AF, but shit works. JS code in C# FTW
        try {
            for (int i = 0; i < files.entries.Count; i++) {
                FileInformation tmp_file = new FileInformation();
                tmp_file.FileName = files.entries[i]["name"];
                tmp_file.FilePath = files.entries[i]["path_lower"];
                tmp_file.FileType = files.entries[i][".tag"];
                string client_modified = files.entries[i]["client_modified"];
                string server_modified = files.entries[i]["server_modified"];
                if (files.entries[i][".tag"] == "folder") {
                    tmp_file.ChangedDate = new DateTime();
                    tmp_file.CreationDate = new DateTime(); 
                } else {
                    //TODO: (nkc) parse date later
                    tmp_file.ChangedDate = DateTime.ParseExact(client_modified, "MM/dd/yyyy HH:mm:ss",  new CultureInfo("en-DK"));
                    tmp_file.CreationDate = DateTime.ParseExact(server_modified, "MM/dd/yyyy HH:mm:ss",  new CultureInfo("en-DK"));
                }
                tmp_file.UID = user;
                tmp_file.SID = service;
                filelist.Add(tmp_file);
            }

        } catch (Exception e) {
            Console.WriteLine(e);
        }
        
        await _file_repo.RemoveUserCache(user, service);
        await _file_repo.AddFileInformationRangeAsync(filelist);
        await _file_repo.SaveDbChangesAsync();

        return filelist;
    }
}