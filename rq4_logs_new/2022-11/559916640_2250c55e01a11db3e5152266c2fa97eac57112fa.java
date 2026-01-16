package net.nurigo.java_sdk.examples.Message;

import java.util.HashMap;
import org.json.simple.JSONObject;
import net.nurigo.java_sdk.api.Message;
import net.nurigo.java_sdk.exceptions.CoolsmsException;

/**
 * @class ExampleSend
 * @brief This sample code demonstrate how to send sms through CoolSMS Rest API PHP
 */
public class ExampleSend {
    public static void main(String[] args) {
        String api_key = "NCSI63DGQEUWZPJE";
        String api_secret = "2MP0QF6VI94CIRR0VGDF1RRFQ0QC6Z33";
        Message coolsms = new Message(api_key, api_secret);

        // 4 params(to, from, type, text) are mandatory. must be filled
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("to", "01048005799");
        params.put("from", "01048005799");
        params.put("type", "SMS");
        params.put("text", "Coolsms Testing Message!");
        params.put("app_version", "test app 1.2"); // application name and version

        try {
            JSONObject obj = (JSONObject) coolsms.send(params);
            System.out.println(obj.toString());
        } catch (CoolsmsException e) {
            System.out.println(e.getMessage());
            System.out.println(e.getCode());
        }
    }
}