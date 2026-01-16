package com.example.dormitory_magician;

import android.app.ActionBar;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.graphics.drawable.BitmapDrawable;
import android.os.Bundle;
import android.util.DisplayMetrics;
import android.view.Display;
import android.view.Gravity;
import android.view.LayoutInflater;
import android.view.View;
import android.view.WindowManager;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.PopupWindow;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import testtime.wheelview.DateUtils;
import testtime.wheelview.JudgeDate;
import testtime.wheelview.ScreenInfo;
import testtime.wheelview.WheelMain;



public class LifeCycleRelease extends Activity implements View.OnClickListener{
    private TextView dateTimePicker;
    private TextView tv_center;
    private WheelMain wheelMainDate;
    private Button btn_release;
    private Spinner spinner;
    private EditText et_place;
    private EditText et_detail;
    private String activityType;
    private String activityTime;
    private String activityPlace;
    private String activityDetail;
    private String URL;
    private String request;
    private java.text.DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.lifecycle_release);
        tv_center=(TextView)findViewById(R.id.tv_center);
        et_place=(EditText)findViewById(R.id.activity_release_place);
        et_detail=(EditText)findViewById(R.id.activity_release_detail);

        spinner=(Spinner)findViewById(R.id.activity_release_type);
        final String[] activity_type = getResources().getStringArray(R.array.activity_type);
        ArrayAdapter<String> typeAdapter = new ArrayAdapter<String>(this,android.R.layout.simple_spinner_item,activity_type);
        typeAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinner.setAdapter(typeAdapter);
        spinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int pos, long id) {
                activityType = activity_type[pos];
                Toast.makeText(LifeCycleRelease.this,activityType,Toast.LENGTH_SHORT).show();
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });

        btn_release=(Button)findViewById(R.id.submit_btn);
        btn_release.setOnClickListener(this);
        dateTimePicker=(TextView)findViewById(R.id.datetime_picker);
        dateTimePicker.setOnClickListener(this);
    }

    public void backgroundAlpha(float bgAlpha) {
        WindowManager.LayoutParams lp = getWindow().getAttributes();
        lp.alpha = bgAlpha;
        getWindow().setAttributes(lp);
    }
    @Override
    public void onClick(View v) {
        int i = v.getId();
        switch (i){
            case R.id.datetime_picker:
                showPopupWindow();
                break;
            case R.id.submit_btn:
                releaseActivity();
                break;
            default:
                break;
        }
    }

    class popOnDismissListener implements PopupWindow.OnDismissListener {
        @Override
        public void onDismiss() {
            backgroundAlpha(1f);
        }
    }

    public void showPopupWindow(){
        WindowManager manager = (WindowManager)getSystemService(Context.WINDOW_SERVICE);
        Display defaultDisplay = manager.getDefaultDisplay();
        DisplayMetrics outMetrics = new DisplayMetrics();
        defaultDisplay.getMetrics(outMetrics);
        int width = outMetrics.widthPixels;
        View menuView = LayoutInflater.from(this).inflate(R.layout.show_popup_window,null);
        final PopupWindow mPopupWindow = new PopupWindow(menuView, (int)(width*0.8),
                ActionBar.LayoutParams.WRAP_CONTENT);
        ScreenInfo screenInfoDate = new ScreenInfo(this);
        wheelMainDate = new WheelMain(menuView, true);
        wheelMainDate.screenheight = screenInfoDate.getHeight();
        String time = DateUtils.currentMonth();
        Calendar calendar = Calendar.getInstance();
        if (JudgeDate.isDate(time, "yyyy-MM-DD")) {
            try {
                calendar.setTime(new Date(time));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        wheelMainDate.initDateTimePicker(year, month, day, hours,minute);
        final String currentTime = wheelMainDate.getTime();
        mPopupWindow.setAnimationStyle(R.style.AnimationPreview);
        mPopupWindow.setTouchable(true);
        mPopupWindow.setFocusable(true);
        mPopupWindow.setBackgroundDrawable(new BitmapDrawable());
        mPopupWindow.showAtLocation(tv_center, Gravity.CENTER, 0, 0);
        mPopupWindow.setOnDismissListener(new popOnDismissListener());
        backgroundAlpha(0.6f);
        TextView tv_cancle = (TextView) menuView.findViewById(R.id.tv_cancle);
        TextView tv_ensure = (TextView) menuView.findViewById(R.id.tv_ensure);
        TextView tv_pop_title = (TextView) menuView.findViewById(R.id.tv_pop_title);
        tv_pop_title.setText("选择起始时间");
        tv_cancle.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                mPopupWindow.dismiss();
                backgroundAlpha(1f);
            }
        });
        tv_ensure.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                activityTime = wheelMainDate.getTime();
                try {
                    Date begin = dateFormat.parse(currentTime);
                    Date end = dateFormat.parse(activityTime);
                    dateTimePicker.setText(DateUtils.formateStringH(activityTime, DateUtils.yyyyMMddHHmm));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                mPopupWindow.dismiss();
                backgroundAlpha(1f);
            }
        });
    }

    public void releaseActivity(){
        //Intent intent = new Intent(LifeCycleRelease.this,LifeCycleFragment.class);
        //startActivity(intent);
        activityPlace=et_place.getText().toString().trim();
        activityDetail=et_detail.getText().toString().trim();
        request = URL+"?action=add&"+ "Member_Name=66&"
                + "Activity_Date="+activityTime
                +"&Site="+activityType
                +"&type="+activityPlace
                +"&Activity_Desc="+activityDetail;
        finish();
    }
}