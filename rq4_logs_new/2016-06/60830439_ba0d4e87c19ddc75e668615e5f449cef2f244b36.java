package easyrun.activity;

import android.app.Activity;
import android.app.ProgressDialog;
import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.view.Gravity;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.RadioGroup;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.FormEncodingBuilder;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import easyrun.bean.EventBean;
import easyrun.server.SendDataToServerByOKHttp;
import easyrun.server.SendDateToServer;
import easyrun.util.R;
import easyrun.util.ServerData;


public class LoginActivity extends Activity{

    private EditText account;
    private EditText password;
    private Button loginButton;
    private TextView newUser;
    private ProgressDialog dialog;// 创建等待框
    private final OkHttpClient client = new OkHttpClient();
    private String username;
    private  String passwd;
    private  String path;
    private RadioGroup chooseUser;
    private String who;


    Handler handler= new Handler(Looper.getMainLooper()){
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case SendDataToServerByOKHttp.SEND_SUCCESS:
                    Toast.makeText(LoginActivity.this, "登录成功", Toast.LENGTH_SHORT).show();
                    break;
                case SendDataToServerByOKHttp.SEND_FAIL:
                    Toast.makeText(LoginActivity.this, "用户名或密码错误", Toast.LENGTH_SHORT).show();
                    break;
                case SendDataToServerByOKHttp.SEND_NULL:
                    Toast.makeText(LoginActivity.this, "用户名或密码不能为空", Toast.LENGTH_SHORT).show();
                case SendDataToServerByOKHttp.SEND_NOTCONNECT:
                    Toast.makeText(LoginActivity.this, "网络故障，请重试", Toast.LENGTH_SHORT).show();
                default:
                    break;
            }
        }
    };

    @Override
    public void onCreate(Bundle savedInstanceState) {
        getActionBar().hide();
        super.onCreate(savedInstanceState);
        getWindow().setSoftInputMode(WindowManager.LayoutParams.SOFT_INPUT_ADJUST_PAN);//解决软键盘遮挡
        setContentView(R.layout.login);
        account = (EditText)findViewById(R.id.account);
        password = (EditText)findViewById(R.id.password);
        loginButton = (Button)findViewById(R.id.LoginButton);
        loginButton.setOnClickListener(loginListener);
        newUser = (TextView)findViewById(R.id.newUser);
        newUser.setOnClickListener(newUserListener);
        chooseUser=(RadioGroup)findViewById(R.id.chooseUser);
        chooseUser.setOnCheckedChangeListener(mylistener);

    }

    RadioGroup.OnCheckedChangeListener mylistener=new RadioGroup.OnCheckedChangeListener(){
        @Override
    public void onCheckedChanged(RadioGroup group,int checkId)
        {
            if(checkId==R.id.chooseUser)
            {
                who="1";
            }
            else
            {
                who="2";
            }
        }
    };
    TextView.OnClickListener newUserListener = new TextView.OnClickListener(){
        @Override
        public void onClick(View v) {
            if(!checkNetwork())
            {
                Toast toast = Toast.makeText(LoginActivity.this,"网络未连接", Toast.LENGTH_SHORT);
                toast.setGravity(Gravity.CENTER, 0, 0);
                toast.show();
                Intent intent = new Intent();
                intent.setClass(LoginActivity.this, NonetActivity.class);
                startActivity(intent);
                finish();
            }
            else {
                Intent intent = new Intent();
                intent.setClass(LoginActivity.this, RegisterActivity.class);
                startActivity(intent);
                //finish();
            }

        }
    };

    //登录按钮点击事件
    Button.OnClickListener loginListener = new Button.OnClickListener(){
        @Override
        public void onClick(View v) {
            switch (v.getId()) {
                case R.id.LoginButton:
                    // 检测网络
                    if (!checkNetwork()) {
                        Toast toast = Toast.makeText(LoginActivity.this,"网络未连接", Toast.LENGTH_SHORT);
                        toast.setGravity(Gravity.CENTER, 0, 0);
                        toast.show();
                        Intent intent = new Intent();
                        intent.setClass(LoginActivity.this, NonetActivity.class);
                        startActivity(intent);
                        finish();
                        break;
                    }
                    /*// 提示框
                    dialog = new ProgressDialog(getParent());
                    dialog.setTitle("提示");
                    dialog.setMessage("登录中...");
                    dialog.setCancelable(false);
                    dialog.show();*/
                    // 创建子线程，进行Post传输
                    new Thread() {
                        public void run() {
                            username = account.getText().toString();//账号
                            passwd = password.getText().toString();//密码
                            path=ServerData.BaseURL + "LoginServer";
                            if (username.equals("")||passwd.equals("")) {
                                handler.sendEmptyMessage(SendDateToServer.SEND_NULL);
                            }else{
                                Map<String, String> map = new HashMap<String, String>();
                                map.put("account",username);
                                map.put("password",passwd);
                                try {
                                    //OKHttp
                                    //普通键值对post用法
                                    RequestBody formBody = new FormEncodingBuilder()
                                            .add("account", username)
                                            .add("password", passwd)
                                            .add("who",who)
                                            .build();

                                    Request request = new Request.Builder()
                                            .url(path)
                                            .post(formBody)
                                            .build();
                                    client.newCall(request).enqueue(new Callback() {
                                        @Override
                                        public void onFailure(Request request, IOException e) {
                                            Toast toast = Toast.makeText(LoginActivity.this, "网络未连接", Toast.LENGTH_SHORT);
                                            toast.setGravity(Gravity.CENTER, 0, 0);
                                            toast.show();
                                            e.printStackTrace();
                                        }

                                        @Override
                                        public void onResponse(Response response) throws IOException {
                                            if (!response.isSuccessful()) {
                                                throw new IOException("Unexpected code " + response);
                                            } else {
                                                byte[] b = response.body().bytes(); //获取数据的bytes
                                                String Result = new String(b, "GB2312"); //然后将其转为gb2312
                                                if (Result.equals("failed")) {
                                                    handler.sendEmptyMessage(SendDataToServerByOKHttp.SEND_FAIL);
                                                } else {
                                                    Intent intent = new Intent();
                                                    Bundle bundle = new Bundle();
                                                    //传递登录成功的账号密码
                                                    bundle.putString("account", username);
                                                    bundle.putString("password", passwd);
                                                    intent.putExtras(bundle);
                                                    intent.setClass(LoginActivity.this, MainActivity.class);
                                                    startActivity(intent);
                                                    finish();
                                                }
                                            }
                                        }
                                    });
                                    //OKHttp

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }.start();
                    break;
            }
        }
    };

    private boolean checkNetwork() {// 检测网络
        ConnectivityManager connManager = (ConnectivityManager) getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connManager.getActiveNetworkInfo() != null) {
            return connManager.getActiveNetworkInfo().isAvailable();
        }
        return false;
    }

}