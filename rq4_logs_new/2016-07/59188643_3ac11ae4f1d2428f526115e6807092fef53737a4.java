package com.mmdj.textanalyzer;

import android.os.AsyncTask;
import android.os.Build;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;

public class OpenHTTPConnection extends AsyncTask<String, Void, String> {
    private static final String CHARSET = "UTF-8";;
    private MainActivity main = new MainActivity();

    @Override
    protected String doInBackground(String... str) {
        StringBuilder content;
        String currentURL = str[0];
        String result = null;


        try {
            content = OpenHttpConnection(currentURL, CHARSET);
            // Log.d(main.getLogTag(), "content1: done");
            // Log.d(main.getLogTag(), "currentURL = " + currentURL);

            String realCharset = getCharset(content);



                if (!Objects.equals(CHARSET, realCharset)&&Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT) {
                    content = OpenHttpConnection(currentURL, realCharset);
                    Log.d(main.getLogTag(), "Encoding HTML: " + realCharset);
                }


            result = getContentWithoutHTML(content);
            //Log.d(main.getLogTag(), "resultWithoutHTML: " + result);


        } catch (IOException ex) {
            Log.d(main.getLogTag(), "IOException in Background");
        }

        return result;
    }


    private StringBuilder OpenHttpConnection(String strURL, String charset) throws IOException {

        HttpURLConnection conn;
        InputStream inputStream;
        StringBuilder content = new StringBuilder();//(--)

        try {
            URL url = new URL(strURL);
            Log.d(main.getLogTag(), "URL = " + url.toString());

            conn = (HttpURLConnection) url.openConnection();

            // just want to do an HTTP GET here
            conn.setRequestMethod("GET");


            // give it 10 seconds to respond
            conn.setReadTimeout(10 * 1000);

            conn.connect();
            inputStream = conn.getInputStream();

            InputStreamReader ISReader = new InputStreamReader(inputStream, charset);
            BufferedReader bufferedReader = new BufferedReader(ISReader);

            String line;

            while ((line = bufferedReader.readLine()) != null) {
                content.append(line).append("\n");
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        return content;

    }


    private String getContentWithoutHTML(StringBuilder content) {
        //deleting all tags from HTML:
        return content.toString().replaceAll("(<script(\\s|\\S)*?<\\/script>)|" +
                "(<style(\\s|\\S)*?<\\/style>)|" +
                "(<!--(\\s|\\S)*?-->)|" +
                "(<\\/?(\\s|\\S)*?>)|" +
                "[\\n]", " ");
    }

    private String getCharset(StringBuilder content) {
        // String charset = content.toString().replaceFirst("charset\\s?=[\\s\"']*([^\\s\"'/>]*)\"", "");
        String[] charsetArray = content.toString().split("charset=");
        //Log.d(main.getLogTag(), "charsetArray[1]: " + charsetArray[1]);

        String charset = "UTF-8";

        if (charsetArray != null) {
            charsetArray = charsetArray[1].split("\"");
            charset = charsetArray[0];


            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT && Objects.equals(charset, "")) {
                charset = charsetArray[1];
               // Log.d(main.getLogTag(), "charsetArrayIf[0]: " + charsetArray[0]);
                Log.d(main.getLogTag(), "charsetArrayIf[1]: " + charsetArray[1]);
            }

           // Log.d(main.getLogTag(), "charset: " + charset);

        }
        return charset;
    }


}