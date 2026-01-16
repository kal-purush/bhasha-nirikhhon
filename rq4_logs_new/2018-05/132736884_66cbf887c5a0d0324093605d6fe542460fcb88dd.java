package cn.bupt.runningplanner;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.TextView;

import cn.bupt.runningplanner.classes.HomePage.alert.AlertMessage;
import cn.bupt.runningplanner.classes.HomePage.alert.AlertableAppCompatActivity;

public class ModifyInfoActivity extends AlertableAppCompatActivity {
    private ModifyInfoActivity self = this;

    private String title;
    private String origin;

    View.OnClickListener onClickListener = new View.OnClickListener() {
        @Override
        public void onClick(View v) {
            Intent intent = null;
            switch (v.getId()) {
                case R.id.modify_delete:
                    ((TextView)findViewById(R.id.modify_origin)).setText("");
                    break;
                case R.id.modify_cancel:
                    intent = new Intent();

                    break;
                case R.id.modify_finish:
                    intent = new Intent();
                    changeInfo();
                    break;
                default:
                    if (intent != null) {
                        intent.setClass(self, ModifyInfoActivity.class);
                        startActivity(intent);
                        finish();
                    }
                    break;
            }
        }
    };

    private void changeInfo() {
        if (title == "昵称") {

        }
        else if (title == "年龄") {
            try {
                int age = Integer.parseInt(((EditText)findViewById(R.id.modify_origin)).getText().toString());
            } catch (Exception ex) {
                alert(new AlertMessage("请正确输入年龄", ""));
            }
        }
        else if (title == "性别") {

        }
        else if (title == "身高(CM)") {

        }
        else if (title == "体重(KG)") {

        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_modify_info);
        title = getIntent().getStringExtra("title");
        origin = getIntent().getStringExtra("origin");
        ((TextView)findViewById(R.id.modify_title)).setText(title);
        ((TextView)findViewById(R.id.modify_origin)).setText(origin);
        ((TextView)findViewById(R.id.modify_cancel)).setOnClickListener(onClickListener);
        ((TextView)findViewById(R.id.modify_finish)).setOnClickListener(onClickListener);
        ((ImageView)findViewById(R.id.modify_delete)).setOnClickListener(onClickListener);
    }
}



//            switch (v.getId()) {
//                case R.id.btn_savedata:
//                    int age, weight, height;

//
//                    if (age <= 0 || age >= 100) {
//                        alert(new AlertMessage("请正确输入年龄", ""));
//                        break;
//                    }
//
//                    try {
//                        weight = Integer.parseInt(((EditText) findViewById(R.id.input_weight)).getText().toString());
//                    } catch (Exception ex) {
//                        alert(new AlertMessage("请正确输入体重", ""));
//                        break;
//                    }
//
//                    if (weight <= 0 || weight >= 500) {
//                        alert(new AlertMessage("请正确输入体重", ""));
//                        break;
//                    }
//
//                    try {
//                        height = Integer.parseInt(((EditText) findViewById(R.id.input_height)).getText().toString());
//                    } catch (Exception ex) {
//                        alert(new AlertMessage("请正确输入身高", ""));
//                        break;
//                    }
//
//                    if (height <= 100 || height >= 250) {
//                        alert(new AlertMessage("请正确输入身高", ""));
//                        break;
//                    }
//
//
//                    info.name=((EditText)findViewById(R.id.input_name)).getText().toString();
//                    info.age = age;
//                    info.weight = weight;
//                    info.height = height;
//
//                    String mail="123456789@qq.com";
//                    SharedPreferences pref = getSharedPreferences("data", Context.MODE_PRIVATE);
//                    mail= pref.getString("email","123456789@qq.com");
//                    SQLiteDatabase db = helper.getWritableDatabase();
//                    Cursor cursor = db.rawQuery("select * from PersonInformation where Email=?", new String[]{mail});
//                    boolean result =cursor.moveToNext();
//
//                    if(result){
//                        db.execSQL("UPDATE PersonInformation SET userName=?,Age=?,Weight=?,High=?,Sex=? where Email=?", new Object[]{info.name, info.age, info.weight, info.height,info.sex,mail});
//                        cursor.close();
//                        db.close();
//                    }
//
//                    Intent intent = new Intent();
//                    intent.setClass(self, MainActivity.class);
//                    startActivity(intent);
//                    finish();
//                    break;
//                default:
//                    break;
//            }
//        }