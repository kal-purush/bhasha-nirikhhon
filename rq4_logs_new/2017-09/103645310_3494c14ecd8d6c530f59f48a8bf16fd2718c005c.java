package com.quickcanteen.quickcanteen.activities.userinformation;

import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;
import com.quickcanteen.quickcanteen.R;
import com.quickcanteen.quickcanteen.actions.user.IUserAction;
import com.quickcanteen.quickcanteen.actions.user.impl.UserActionImpl;
import com.quickcanteen.quickcanteen.activities.BaseActivity;
import com.quickcanteen.quickcanteen.utils.BaseJson;

public class EditUserInfo extends BaseActivity {

    private TextView checkPassword,correctInfo;
    private Button confirmEdit;
    private Handler handler=new Handler();
    private String message;
    private IUserAction userAction;
    private Intent intent;
    private String infoType;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_personalinfo);
        BaseActivity.initializeTop(this, true, "修改个人信息");
        checkPassword=(TextView)findViewById(R.id.checkPassword);
        correctInfo=(TextView)findViewById(R.id.correctInfo);
        confirmEdit=(Button)findViewById(R.id.confirmEditInfo);
        userAction = new UserActionImpl(this);
        intent = getIntent();
        infoType = intent.getStringExtra("infoType");
        confirmEdit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String checkPasswordString = checkPassword.getText().toString();
                String correctInfoString = correctInfo.getText().toString();
                if(checkPasswordString.length()==0||correctInfoString.length()==0){
                    Toast.makeText(EditUserInfo.this, "请填写信息", Toast.LENGTH_SHORT).show();
                }
                else{
                    new Thread(new ChangeInfoThread(checkPasswordString,infoType,correctInfoString)).start();
                }
            }
        });
    }
    class ChangeInfoThread implements Runnable{
        private String checkPasswordString;
        private String correctInfoString;
        private String infoType;
        int userID = userAction.getCurrentUserID();
        public ChangeInfoThread(String checkPasswordString,String infoType,String correctInfoString) {
            this.checkPasswordString = checkPasswordString;
            this.correctInfoString = correctInfoString;
            this.infoType = infoType;
        }
        @Override
        public void run() {
            message="修改成功";
            try{
                BaseJson baseJson = userAction.editUserInfo(checkPasswordString,userID,infoType,correctInfoString);
                message = baseJson.getErrorMessage();
                finish();
            }catch (Exception e){
                message="连接错误";
            }
            handler.post(new Runnable() {
                @Override
                public void run() {
                    Toast.makeText(EditUserInfo.this, message, Toast.LENGTH_SHORT).show();
                }
            });
        }
    }
}