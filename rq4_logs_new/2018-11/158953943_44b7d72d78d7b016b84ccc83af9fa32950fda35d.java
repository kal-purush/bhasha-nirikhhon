package cremedelacreme.tickats;

import android.content.Context;
import android.icu.util.Output;
import android.os.AsyncTask;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.Buffer;

public class BackgroundWorker extends AsyncTask<String,Void,String> {

    private TextView textView;
    Context context;


    BackgroundWorker (Context ctx) {
        context = ctx;
    }

    @Override
    protected String doInBackground(String... params) {

        String type = params[0];
        String login_url = "https://tickats.live/login.php";
        String update_location_url = "https://tickats.live/updatelocation.php";
                        //For my (Jeramy's) screen
        String HeavyEquipment_URL = "https://tickats.live/DisplayHeavyEquipment.php";
        String LightEquipment_URL = "https://tickats.live/DisplayLightEquipment.php";
        String WorkerInfo_URL = "https://tickats.live/DisplayWorkerInfo.php";
        String HeaderInfo_URL = "https://tickats.live/DisplayDataWorksiteLocation.php";


        if (type.equals("login")) {

            try {
                String user_name = params[1];
                String password = params[2];
                URL url = new URL(login_url);
                URLConnection conn = url.openConnection();
                conn.setDoOutput(true);
                conn.setDoInput(true);
                OutputStream outputStream = conn.getOutputStream();
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                String post_data = URLEncoder.encode("user_name", "UTF-8") + "=" + URLEncoder.encode(user_name, "UTF-8") + "&"
                        + URLEncoder.encode("password", "UTF-8") + "=" + URLEncoder.encode(password, "UTF-8");
                bufferedWriter.write(post_data);
                bufferedWriter.flush();
                bufferedWriter.close();
                InputStream inputStream = conn.getInputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "iso-8859-1"));
                String result = "";
                String line = "";
                while ((line = bufferedReader.readLine()) != null) {
                    result += line;
                }
                bufferedReader.close();
                inputStream.close();
                outputStream.close();

                return result;

            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if (type.equals("TicketDetails")){
            try{
                String fwid = params[1];
                URL url = new URL(TicketDetails_URL);
                URLConnection conn = url.openConnection();
                conn.setDoOutput(true);
                conn.setDoInput(true);
                OutputStream outputStream = conn.getOutputStream();
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream,"UTF-8" ));
                String post_data = URLEncoder.encode("fwid", "UTF-8") + "=" + URLEncoder.encode(fwid, "UTF-8");
                bufferedWriter.write(post_data);
                bufferedWriter.flush();
                bufferedWriter.close();
                InputStream inputStream = conn.getInputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "iso-8859-1"));
                String result = "";
                String line = "";
                while ((line = bufferedReader.readLine()) !=null ){
                    result += line;
                }
                bufferedReader.close();
                inputStream.close();
                outputStream.close();
                return result;

            }
            catch(MalformedURLException e){
                e.printStackTrace();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }


        if(type.equals("update")){

        }

        if (type.equals("viewAllMyTickets")){

        }

        if (type.equals("view")){

        }
        return null;
    }

}