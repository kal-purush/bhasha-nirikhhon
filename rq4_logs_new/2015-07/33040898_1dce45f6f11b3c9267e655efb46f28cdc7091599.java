package dk.jimmikristensen.aaws.domain.github;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.squareup.okhttp.Headers;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class GithubConnector {
    
    private OkHttpClient client;
    private final String ACCESS_TOKEN = "c74754c7013f3f897bf176ed48e915769fae9da6";
    private final String USER_AGENT = "Asciidoc-ws-1.0";
    private final String GITAPI = "https://api.github.com";
    private List<Map<String, String>> adocs;
    private List<Map<String, String>> assets;
    private final String[] ACCEPTED_FILES = {"pdf", "png", "jpg"};
    
    public GithubConnector() {
        client = new OkHttpClient();
        adocs = new ArrayList<>();
        assets = new ArrayList<>();
    }

    public void fetchCommits(String owner, String repo, Date date) throws IOException, ParseException, java.text.ParseException {
        String url = GITAPI + "/repos/"+owner+"/"+repo+"/events";
        scanCommits(url, date);
    }
    
    private void scanCommits(String url, Date date) throws IOException, ParseException, java.text.ParseException, ClassCastException {
        Response resp = callGithub(url);
        String respBody = resp.body().string().trim();
        
        JSONParser jsonParser = new JSONParser();
        Object obj = jsonParser.parse(respBody);

        JSONArray tree = (JSONArray) obj;
        Iterator<JSONObject> itr = tree.iterator();
        
        while (itr.hasNext()) {
            JSONObject jsonObject = itr.next();
            String type = (String) jsonObject.get("type");
            String created = (String) jsonObject.get("created_at");
            
            DateFormat ghFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date ghCommitDate = ghFormat.parse(created);
            
            // look for push events after a given date
            if (type.equals("PushEvent") && ghCommitDate.after(date)) {
                JSONObject payload = (JSONObject) jsonObject.get("payload");
                JSONArray commits = (JSONArray) payload.get("commits");
                Iterator<JSONObject> commitsItr = commits.iterator();
                
                while (commitsItr.hasNext()) {
                    JSONObject commit = commitsItr.next();
                    String commitUrl = (String) commit.get("url");
                    scanCommit(commitUrl);
                }
            }
        }
    }
    
    private void scanCommit(String url) throws IOException, ParseException, ClassCastException {
        Response resp = callGithub(url);
        String respBody = resp.body().string().trim();
        
        JSONParser jsonParser = new JSONParser();
        Object obj = jsonParser.parse(respBody);
        
        JSONObject rootObj = (JSONObject) obj;


            JSONArray files = (JSONArray) rootObj.get("files");  
            Iterator<JSONObject> filesItr = files.iterator();
            
            while (filesItr.hasNext()) {
                JSONObject file = filesItr.next();
                String sha = (String) file.get("sha");
                String path = (String) file.get("filename");
                String name = path.substring(path.lastIndexOf("/")+1);
                String downloadUrl = (String) file.get("raw_url");
                System.out.println(sha);
                System.out.println(path);
                System.out.println(name);
                System.out.println(downloadUrl);
            }

    }
    
    public void fetchAll(String owner, String repo) throws IOException, ParseException {
        String url = GITAPI + "/repos/"+owner+"/"+repo+"/contents";
        scanRepo(url);
    }
    
    private void scanRepo(String url) throws IOException, ParseException, ClassCastException {        
        Response resp = callGithub(url);
        String respBody = resp.body().string().trim();

        JSONParser jsonParser = new JSONParser();
        Object obj = jsonParser.parse(respBody);
        
        JSONArray tree = (JSONArray) obj;
        Iterator<JSONObject> itr = tree.iterator();
        
        while (itr.hasNext()) {
            JSONObject jsonObject = itr.next();
            String name = (String) jsonObject.get("name");
            String type = (String) jsonObject.get("type");
            String downloadUrl = (String) jsonObject.get("download_url");
            String urlRef = (String) jsonObject.get("url");
            String sha = (String) jsonObject.get("sha");
            String path = (String) jsonObject.get("path");
            
            if (name != null && type != null) {
                if (type.equals("file") && name.endsWith(".adoc")) {
                    Map<String, String> meta = new HashMap<String, String>();
                    meta.put("name", name);
                    meta.put("path", path);
                    meta.put("sha", sha);
                    meta.put("downloadUrl", downloadUrl);
                    adocs.add(meta);
                    
                } else if (type.equals("file")) {
                    for (String ext : ACCEPTED_FILES) {
                        if (name.endsWith("."+ext)) {
                            Map<String, String> meta = new HashMap<String, String>();
                            meta.put("name", name);
                            meta.put("path", path);
                            meta.put("sha", sha);
                            meta.put("downloadUrl", downloadUrl);
                            assets.add(meta);
                        }
                    }
                    
                } else if (type.equals("dir") && urlRef != null) {
                    scanRepo(urlRef);
                }
            }
        }
    }
    
    private Response callGithub(String url) throws IOException {
        HttpUrl httpUrl = HttpUrl.parse(url)
                .newBuilder()
//                .addEncodedQueryParameter("access_token", ACCESS_TOKEN)
                .build();
        
        Request req = new Request
                .Builder()
                .addHeader("User-Agent", USER_AGENT)
                .url(httpUrl)
                .build();
        
        Response resp = client.newCall(req).execute();
        Headers headers = resp.headers();
        System.out.println(headers.get("X-RateLimit-Limit"));
        System.out.println(headers.get("X-RateLimit-Remaining"));
        System.out.println(headers.get("X-RateLimit-Reset"));
        
        return resp;
    }
}

