package com.example.pulopo.gpt;

import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ContentGPT {
    public  String getContentGPt(String textPost) {

        String text = "";
        String apiKey = "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";
        String endpoint = "https://api.openai.com/v1/engines/gpt-3.5-turbo-instruct/completions";

        OkHttpClient client = new OkHttpClient();
        String rs = textPost.replace('\"', ' ').replace('{', ' ').replace('}', ' ').replace(',', ' ').replace('[', ' ')
                .replace(']', ' ').replace(':', ' ').replace('/', ' ').replace('\\', ' ').replace('\'', ' ');

        String input = "Trả lời ngắn về:" + rs.replace("\n", " ");

        byte[] inputByte = input.getBytes();

        String requestInput = new String(inputByte, StandardCharsets.UTF_8);
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\"prompt\": \"" + input + "\", \"max_tokens\": 80}");
        Request request = new Request.Builder().url(endpoint).post(body).addHeader("Authorization", "Bearer " + apiKey)
                .build();

        try {
            Response response = client.newCall(request).execute();
            byte[] responseBodyBytes = response.body().bytes();
            String responseBody = new String(responseBodyBytes, StandardCharsets.UTF_8);

            JSONObject json = new JSONObject(responseBody);
            System.out.println(json);
            JSONArray choices = json.getJSONArray("choices");
            JSONObject choice = choices.getJSONObject(0);
            text = choice.getString("text");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return text;

    }


}