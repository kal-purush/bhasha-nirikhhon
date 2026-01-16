package app.com.lsl.imagesend.Task;

import android.util.Log;

import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import app.com.lsl.imagesend.base.Image64Base;


/** 执行注册业务
 * Created by M1308_000 on 2017/4/20.
 */

public class RegisterTask {

    public static final String IP = "192.168.1.101";
    public static final int PORT = 54321;
    private static final String TYPE = "register";

    public static int Register(String name, String Img_path, int psw) {
        Log.e("RegisterTask:","name:" + name);
        // 将图片的信息转化为base64编码
        String imgStr = Image64Base.getImageStr(Img_path);
        int isRegSuccess = 0;
        while (true){
            Socket socket = null;
            try {
                //创建一个流套接字并将其连接到指定主机上的指定端口号 
                socket = new Socket(IP, PORT);
                Log.e("RegisterTask", "连接已经建立");
                // 向服务器端发送数据 
                Map<String, String> map = new HashMap<>();
                map.put("name",name);
                map.put("img",imgStr);
                map.put("psw",psw + "");
                map.put("type", TYPE);

                // 将json转化为String类型    

                JSONObject json = new JSONObject(map);
                String jsonStr = "";
                jsonStr = json.toString();

                // 将String转化为byte[]  
                byte[] jsonByte = jsonStr.getBytes();
                DataOutputStream ops = null;
                ops = new DataOutputStream(socket.getOutputStream());
                Log.e("RegisterTask", "发送的数据长度为：" + jsonByte.length);
                ops.write(jsonByte);
                ops.flush();
                Log.e("RegisterTask", "传输数据完毕");

                //socket.shutdownOutput();


                // 读取服务器端数据    
                DataInputStream input = null;
                String strinput = "";
                input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                strinput = input.readUTF();
                Log.e("RegisterTask", "输入信息为："+ strinput);
                JSONObject js = new JSONObject((Map) input);
                Log.e("RegisterTask", "" + js.get("isSuccess"));
                isRegSuccess = Integer.parseInt((String)js.get("isSuccess"));

                // 如接收到 "OK" 则断开连接  


                if (js != null) {
                    Log.e("RegisterTask", "客户端将关闭连接");
                    Thread.sleep(500);
                    break;
                }

            } catch (Exception e) {
                Log.e("RegisterTask", "conn time out");
                e.printStackTrace();
                break;
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Exception e) {
                        socket = null;
                        Log.e("RegisterTask", "客户端 finally 异常：" + e.getMessage());

                    }
                }
            }
        }
        return isRegSuccess;
    }
}