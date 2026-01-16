import org.json.JSONObject;

public class Main {

    public static void main(String[] args) {
        String response = logIn();
        JSONObject jsonObject = new JSONObject(response);
        int sessionId = jsonObject.getInt("sessionId");

        task1(sessionId);
    }

    private String BASE_URL; // Base URL (address) of the server

    /**
     * Create an HTTP POST
     *
     * @param host Will send request to this host: IP address or domain
     * @param port Will use this port
     */
    public Main(String host, int port) {
        BASE_URL = "http://" + host + ":" + port + "/";
    }

    /**
     * Post email and phone number to the web server.
     */
    private static String logIn(){
        String url = "dkrest/auth";
        String phone = "47806366";
        String email = "emilva@stud.ntnu.no";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("email", email);
        jsonObject.put("phone", phone);
        String response = POST.sendPost(url, jsonObject);
        return response;
    }

    public static void task1(int sessionID){
        String response = GET.sendGet("dkrest/gettask/1?sessionId=" + sessionID);
        System.out.println(response);

        JSONObject send = new JSONObject();
        send.put("msg", "Hello");
        send.put("sessionId", sessionID);
        response = POST.sendPost("dkrest/solve", send);
        System.out.println(response);
    }
}
