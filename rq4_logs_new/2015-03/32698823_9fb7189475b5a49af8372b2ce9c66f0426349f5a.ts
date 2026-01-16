///<reference path="Scripts/typings/jquery/jquery.d.ts"/>

class GitHubApi
{
    private baseUrl: string;

    constructor()
    {
        this.baseUrl = "https://api.github.com";
    }

    public GetRepos(username: string, success: any, error: any)
    {
        $.ajax({
            url: this.baseUrl + "/users/" + username + "/repos",
            dataType: "jsonp",
            method: "GET",
            success: success,
            error: error
        });
    }

    public GetPullRequests(username: string, repo: string, success: any, error: any)
    {
        $.ajax({
            url: this.baseUrl + "/repos/" + username + "/" + repo + "/pulls",
            dataType: "jsonp",
            method: "GET",
            success: success,
            error: error
        });
    }
}